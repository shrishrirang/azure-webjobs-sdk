// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Threading;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Sql;
using Microsoft.Azure.WebJobs.Host.Storage;

namespace Microsoft.Azure.WebJobs.Host
{
    internal class LeasorFactory
    {
        public static ILeasor CreateLeasor(string accountName, IStorageAccountProvider storageAccountProvider)
        {
            ILeasor leasor = null;
            SqlLeasor.TryGetAccountAsync(accountName, out leasor);

            if (leasor == null)
            {
                var storageAccount = storageAccountProvider.TryGetAccountAsync(accountName, new CancellationToken()).Result;
                leasor = new BlobLeasor(storageAccountProvider);
            }

            return leasor;
        }
    }
}
