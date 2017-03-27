// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Lease;

namespace Microsoft.Azure.WebJobs.Host
{
    internal interface ILeasor // FIXME: review where LeaseDefinition is not the right choice
    {
        Task<string> TryAcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);

        Task RenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);

        Task ReleaseLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);

        Task WriteLeaseBlobMetadataAsync(LeaseDefinition leaseDefinition, string key, string value, CancellationToken cancellationToken);

        Task<LeaseInformation> ReadLeaseInfoAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);
    }
}
