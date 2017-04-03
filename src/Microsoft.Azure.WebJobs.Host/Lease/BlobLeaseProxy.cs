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
    // Azure Storage Blob based lease implementation
    internal class BlobLeaseProxy : ILeaseProxy
    {
        private readonly IStorageAccountProvider _storageAccountProvider;
        private readonly ConcurrentDictionary<string, IStorageAccount> _storageAccountMap = new ConcurrentDictionary<string, IStorageAccount>(StringComparer.OrdinalIgnoreCase);

        public BlobLeaseProxy(IStorageAccountProvider storageAccountProvider)
        {
            if (storageAccountProvider == null)
            {
                throw new ArgumentNullException("storageAccountProvider");
            }

            _storageAccountProvider = storageAccountProvider;
        }

        /// <summary>
        /// <see cref="ILeaseProxy.TryAcquireLeaseAsync"/>
        /// </summary>
        public async Task<string> TryAcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            try
            {
                return await AcquireLeaseAsync(leaseDefinition, cancellationToken);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// <see cref="ILeaseProxy.AcquireLeaseAsync"/>
        /// </summary>
        public async Task<string> AcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            IStorageBlockBlob lockBlob = null;
            try
            {
                lockBlob = GetBlob(leaseDefinition);

                // Optimistically try to acquire the lease. The blob may not exist yet.
                // If it doesn't exist, we handle the 404, create it, and retry below.
                return await lockBlob.AcquireLeaseAsync(leaseDefinition.Period, leaseDefinition.LeaseId, cancellationToken);
            }
            catch (StorageException exception)
            {
                if (exception.RequestInformation != null)
                {
                    if (exception.RequestInformation.HttpStatusCode == 404)
                    {
                        // No action needed. We will create the blob and retry again.
                    }
                    else if (exception.RequestInformation.HttpStatusCode == 409)
                    {
                        throw new LeaseException(LeaseFailureReason.Conflict, exception);
                    }
                    else
                    {
                        throw new LeaseException(LeaseFailureReason.Unknown, exception);
                    }
                }
                else
                {
                    throw new LeaseException(LeaseFailureReason.Unknown, exception);
                }
            }

            try
            {
                await TryCreateBlobAsync(lockBlob, cancellationToken);
                return await lockBlob.AcquireLeaseAsync(leaseDefinition.Period, null, cancellationToken);
            }
            catch (StorageException exception)
            {
                if (exception.RequestInformation != null &&
                    exception.RequestInformation.HttpStatusCode == 409)
                {
                    throw new LeaseException(LeaseFailureReason.Conflict, exception);
                }

                throw new LeaseException(LeaseFailureReason.Unknown, exception);
            }
        }

        /// <summary>
        /// <see cref="ILeaseProxy.RenewLeaseAsync"/>
        /// </summary>
        public Task RenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            if (leaseDefinition == null)
            {
                throw new ArgumentNullException("leaseDefinition");
            }

            try
            {
                IStorageBlockBlob lockBlob = GetBlob(leaseDefinition);
                var accessCondition = new AccessCondition
                {
                    LeaseId = leaseDefinition.LeaseId
                };

                return lockBlob.RenewLeaseAsync(accessCondition, null, null, cancellationToken);
            }
            catch (StorageException exception)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, exception);
            }
        }

        /// <summary>
        /// <see cref="ILeaseProxy.WriteLeaseMetadataAsync"/>
        /// </summary>
        public async Task WriteLeaseMetadataAsync(LeaseDefinition leaseDefinition, string key, string value, CancellationToken cancellationToken)
        {
            try
            {
                IStorageBlockBlob lockBlob = GetBlob(leaseDefinition);
                lockBlob.Metadata.Add(key, value);

                await lockBlob.SetMetadataAsync(
                    accessCondition: new AccessCondition { LeaseId = leaseDefinition.Name },
                    options: null,
                    operationContext: null,
                    cancellationToken: cancellationToken);
            }
            catch (StorageException exception)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, exception);
            }
        }

        /// <summary>
        /// <see cref="ILeaseProxy.ReleaseLeaseAsync"/>
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
                        throw new LeaseException(LeaseFailureReason.Unknown, exception);
                    }
                }
                else
                {
                    throw new LeaseException(LeaseFailureReason.Unknown, exception);
                }
            }
        }

        /// <summary>
        /// <see cref="ILeaseProxy.ReadLeaseInfoAsync"/>
        /// </summary>
        public async Task<LeaseInformation> ReadLeaseInfoAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            try
            {
                IStorageBlob lockBlob = GetBlob(leaseDefinition);

                await FetchLeaseBlobMetadataAsync(lockBlob, cancellationToken);

                var isLeaseAvailable = lockBlob.Properties.LeaseState == LeaseState.Available &&
                                        lockBlob.Properties.LeaseStatus == LeaseStatus.Unlocked;

                var leaseInformation = new LeaseInformation(isLeaseAvailable, lockBlob.Metadata);

                return leaseInformation;
            }
            catch (StorageException exception)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, exception);
            }
        }

        private static async Task FetchLeaseBlobMetadataAsync(IStorageBlob blob, CancellationToken cancellationToken) // FIXME: metadata read and write functions need to be tested.
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

        private static async Task<bool> TryCreateBlobAsync(IStorageBlockBlob blob, CancellationToken cancellationToken)
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

            // Create the container if it does not exist.
            // Directories need not be created as they are created automatically, if needed.
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

        private IStorageBlockBlob GetBlob(LeaseDefinition leaseDefinition)
        {
            var accountName = leaseDefinition.AccountName;

            // FIXME: what if accountName is null?
            IStorageAccount storageAccount;
            if (!_storageAccountMap.TryGetValue(accountName, out storageAccount))
            {
                storageAccount = _storageAccountProvider.GetAccountAsync(accountName, CancellationToken.None).Result;
                // singleton requires block blobs, cannot be premium
                storageAccount.AssertTypeOneOf(StorageAccountType.GeneralPurpose, StorageAccountType.BlobOnly);
                _storageAccountMap[accountName] = storageAccount;
            }

            IStorageBlobClient blobClient = storageAccount.CreateBlobClient();
            IStorageBlobContainer container = blobClient.GetContainerReference(leaseDefinition.Namespace);

            IStorageBlockBlob blob;
            if (string.IsNullOrWhiteSpace(leaseDefinition.Category))
            {
                blob = container.GetBlockBlobReference(leaseDefinition.Name);
            }
            else
            {
                IStorageBlobDirectory blobDirectory = container.GetDirectoryReference(leaseDefinition.Category);
                blob = blobDirectory.GetBlockBlobReference(leaseDefinition.Name);
            }

            return blob;
        }
    }
}
