// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.WebJobs.Host.Storage
{
    internal class StorageBlobLeasor : ILeasor
    {
        private IStorageAccount _storageAccount;

        public StorageBlobLeasor(IStorageAccount storageAccount)
        {
            _storageAccount = storageAccount;
        }

        public Task<string> TryAcquireLeaseAsync(string leaseNamespace, string leaseId, TimeSpan leasePeriod, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
