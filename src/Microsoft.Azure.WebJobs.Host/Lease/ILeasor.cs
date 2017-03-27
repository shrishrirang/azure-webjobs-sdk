// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Lease;

namespace Microsoft.Azure.WebJobs.Host
{
    internal interface ILeasor
    {
        Task<string> TryAcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);

        Task WriteLeaseBlobMetadata(LeaseDefinition leaseDefinition, string key, string value, CancellationToken cancellationToken);

        Task ReleaseLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);

        Task ReadLeaseBlobMetadata(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);
    }
}
