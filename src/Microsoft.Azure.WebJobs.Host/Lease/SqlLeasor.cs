// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;

// FIXME: fix this whole file
namespace Microsoft.Azure.WebJobs.Host.Sql
{
    internal class SqlLeasor : ILeasor
    {
        public SqlLeasor(string connectionString)
        {
        }

        public Task<string> TryAcquireLeaseAsync(string leaseNamespace, string leaseId, TimeSpan leasePeriod, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public static bool TryParse(string leasorConnectionString, out ILeasor leasor)
        {
            leasor = null;
            return false;
        }
    }
}
