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
        // FIXME: what if no storage connection strings are defined? that should be supported too. Introduce InMemoryLeasor that implements ILeasor.cs
        public static ILeasor CreateLeasor(IStorageAccountProvider storageAccountProvider)
        {
            string accountName = ConnectionStringNames.Leasor;

            if (string.IsNullOrWhiteSpace(AmbientConnectionStringProvider.Instance.GetConnectionString(accountName)))
            {
                accountName = ConnectionStringNames.Storage;
            }

            ILeasor leasor = null;
            SqlLeasor.TryGetAccountAsync(accountName, out leasor);

            if (leasor == null)
            {
                var storageAccount = storageAccountProvider.TryGetAccountAsync(accountName, CancellationToken.None).Result;
                leasor = new BlobLeasor(storageAccountProvider);
            }

            return leasor;
        }


    }
}
