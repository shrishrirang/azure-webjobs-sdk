// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Lease;
using Microsoft.Azure.WebJobs.Host.Storage;
using Microsoft.Azure.WebJobs.Host.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// FIXME
    /// </summary>
    internal class BlobLeasor : ILeasor
    {
        private IStorageAccountProvider _storageAccountProvider;
        private ConcurrentDictionary<string, IStorageBlobDirectory> _lockDirectoryMap = new ConcurrentDictionary<string, IStorageBlobDirectory>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// FIXME
        /// </summary>
        public BlobLeasor(IStorageAccountProvider storageAccountProvider)
        {
            _storageAccountProvider = storageAccountProvider;
        }

        private IStorageBlockBlob GetBlob(LeaseDefinition leaseDefinition)
        {
            IStorageBlobDirectory lockDirectory = GetLockDirectory(leaseDefinition.AccountName, leaseDefinition.Namespace, leaseDefinition.Category);
            return lockDirectory.GetBlockBlobReference(leaseDefinition.LockId);
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public async Task<string> TryAcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            IStorageBlockBlob lockBlob = GetBlob(leaseDefinition);

            //leaseId = await TryAcquireLeaseAsync(lockBlob, lockPeriod, cancellationToken);
            bool blobDoesNotExist = false;
            try
            {
                // Optimistically try to acquire the lease. The blob may not yet
                // exist. If it doesn't we handle the 404, create it, and retry below
                return await lockBlob.AcquireLeaseAsync(leaseDefinition.Period, null, cancellationToken);
            }
            catch (StorageException exception)
            {
                if (exception.RequestInformation != null)
                {
                    if (exception.RequestInformation.HttpStatusCode == 409)
                    {
                        return null;
                    }
                    else if (exception.RequestInformation.HttpStatusCode == 404)
                    {
                        blobDoesNotExist = true;
                    }
                    else
                    {
                        throw;
                    }
                }
                else
                {
                    throw;
                }
            }

            if (blobDoesNotExist)
            {
                await TryCreateAsync(lockBlob, cancellationToken);

                try
                {
                    return await lockBlob.AcquireLeaseAsync(leaseDefinition.Period, null, cancellationToken);
                }
                catch (StorageException exception)
                {
                    if (exception.RequestInformation != null &&
                        exception.RequestInformation.HttpStatusCode == 409)
                    {
                        return null;
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public Task RenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            IStorageBlockBlob lockBlob = GetBlob(leaseDefinition);
            var accessCondition = new AccessCondition
            {
                LeaseId = leaseDefinition.LeaseId
            };

            return lockBlob.RenewLeaseAsync(accessCondition, null, null, cancellationToken);
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public async Task WriteLeaseBlobMetadataAsync(LeaseDefinition leaseDefinition, string key, string value, CancellationToken cancellationToken)
        {
            IStorageBlockBlob lockBlob = GetBlob(leaseDefinition);
            lockBlob.Metadata.Add(key, value);

            await lockBlob.SetMetadataAsync(
                accessCondition: new AccessCondition { LeaseId = leaseDefinition.LockId },
                options: null,
                operationContext: null,
                cancellationToken: cancellationToken);
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public async Task ReleaseLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            try
            {
                IStorageBlockBlob blob = GetBlob(leaseDefinition);
                // Note that this call returns without throwing if the lease is expired. See the table at:
                // http://msdn.microsoft.com/en-us/library/azure/ee691972.aspx
                await blob.ReleaseLeaseAsync(
                    accessCondition: new AccessCondition { LeaseId = leaseDefinition.LeaseId },
                    options: null,
                    operationContext: null,
                    cancellationToken: cancellationToken);
            }
            catch (StorageException exception)
            {
                if (exception.RequestInformation != null)
                {
                    if (exception.RequestInformation.HttpStatusCode == 404 ||
                        exception.RequestInformation.HttpStatusCode == 409)
                    {
                        // if the blob no longer exists, or there is another lease
                        // now active, there is nothing for us to release so we can
                        // ignore
                    }
                    else
                    {
                        throw;
                    }
                }
                else
                {
                    throw;
                }
            }
        }

        // FIXME: make this private .. if possible
        // Also make this FetchLeaseBlobMetadataAsync
        private async Task FetchLeaseBlobMetadataAsync(IStorageBlob blob, CancellationToken cancellationToken)
        {
            try
            {
                await blob.FetchAttributesAsync(cancellationToken);
            }
            catch (StorageException exception)
            {
                if (exception.RequestInformation != null &&
                    exception.RequestInformation.HttpStatusCode == 404)
                {
                    // the blob no longer exists
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public async Task<LeaseInformation> ReadLeaseInfoAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            var leaseInformation = new LeaseInformation
            {
                IsLeaseAvailable = false
            };

            IStorageBlob lockBlob = GetBlob(leaseDefinition);

            await FetchLeaseBlobMetadataAsync(lockBlob, cancellationToken);

            // if the lease is Available, then there is no current owner
            // (any existing owner value is the last owner that held the lease)
            if (lockBlob.Properties.LeaseState != LeaseState.Available ||
                lockBlob.Properties.LeaseStatus != LeaseStatus.Unlocked)
            {
                leaseInformation.IsLeaseAvailable = false;
            }

            leaseInformation.Metadata = lockBlob.Metadata;

            return leaseInformation;
        }


        private static async Task<bool> TryCreateAsync(IStorageBlockBlob blob, CancellationToken cancellationToken)
        {
            bool isContainerNotFoundException = false;

            try
            {
                await blob.UploadTextAsync(string.Empty, cancellationToken: cancellationToken);
                return true;
            }
            catch (StorageException exception)
            {
                if (exception.RequestInformation != null)
                {
                    if (exception.RequestInformation.HttpStatusCode == 404)
                    {
                        isContainerNotFoundException = true;
                    }
                    else if (exception.RequestInformation.HttpStatusCode == 409 ||
                             exception.RequestInformation.HttpStatusCode == 412)
                    {
                        // The blob already exists, or is leased by someone else
                        return false;
                    }
                    else
                    {
                        throw;
                    }
                }
                else
                {
                    throw;
                }
            }

            Debug.Assert(isContainerNotFoundException);
            await blob.Container.CreateIfNotExistsAsync(cancellationToken);

            try
            {
                await blob.UploadTextAsync(string.Empty, cancellationToken: cancellationToken);
                return true;
            }
            catch (StorageException exception)
            {
                if (exception.RequestInformation != null &&
                    (exception.RequestInformation.HttpStatusCode == 409 || exception.RequestInformation.HttpStatusCode == 412))
                {
                    // The blob already exists, or is leased by someone else
                    return false;
                }
                else
                {
                    throw;
                }
            }
        }




        private IStorageBlobDirectory GetLockDirectory(string accountName, string leaseNamespace, string leaseCategory)
        {
            IStorageBlobDirectory storageDirectory = null;
            // FIXME: what if accountName is null?
            if (!_lockDirectoryMap.TryGetValue(accountName, out storageDirectory))
            {
                Task<IStorageAccount> task = _storageAccountProvider.GetAccountAsync(accountName, CancellationToken.None);
                IStorageAccount storageAccount = task.Result;
                // singleton requires block blobs, cannot be premium
                storageAccount.AssertTypeOneOf(StorageAccountType.GeneralPurpose, StorageAccountType.BlobOnly);
                IStorageBlobClient blobClient = storageAccount.CreateBlobClient();
                storageDirectory = blobClient.GetContainerReference(leaseNamespace)
                                       .GetDirectoryReference(leaseCategory);
                _lockDirectoryMap[accountName] = storageDirectory;
            }

            return storageDirectory;
        }
    }
}
