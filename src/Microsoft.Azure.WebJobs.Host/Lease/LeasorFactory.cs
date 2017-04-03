// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Lease;
using Microsoft.Azure.WebJobs.Host.Storage;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// FIXME
    /// </summary>
    internal class LeasorFactory
    {
        public static ILeasor CreateLeasor(IStorageAccountProvider storageAccountProvider)
        {
            ILeasor leasor = null;

            if (SqlLeasor.IsSqlLeaseType())
            {
                leasor = new SqlLeasor();
            }
            else
            {
                leasor = new BlobLeasor(storageAccountProvider);
            }

            return leasor;
        }
    }
}
