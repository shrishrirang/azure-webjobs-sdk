﻿// Copyright (c) .NET Foundation. All rights reserved.
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
    internal class LeaseFactory
    {
        public static ILeaseProxy CreateLeaseProxy(IStorageAccountProvider storageAccountProvider)
        {
            ILeaseProxy leaseProxy = null;

            if (SqlLeaseProxy.IsSqlLeaseType())
            {
                leaseProxy = new SqlLeaseProxy();
            }
            else
            {
                leaseProxy = new BlobLeaseProxy(storageAccountProvider);
            }

            return leaseProxy;
        }
    }
}
