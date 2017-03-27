﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Lease;

// FIXME: fix this whole file
namespace Microsoft.Azure.WebJobs.Host.Sql
{
    internal class SqlLeasor : ILeasor
    {
        public SqlLeasor(string connectionString)
        {
        }

        public Task<string> TryAcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task WriteLeaseBlobMetadata(LeaseDefinition leaseDefinition, string key,
            string value, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task ReleaseLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task ReadLeaseBlobMetadata(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public static bool TryGetAccountAsync(string accountName, out ILeasor leasor)
        {
            leasor = null;
            var connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(accountName);
            if (string.IsNullOrWhiteSpace(connectionString))
                return false;

            return false;
        }
    }
}
