// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Lease;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// FIXME
    /// </summary>
    public interface ILeasor // FIXME: review where LeaseDefinition is not the right choice
    {
        /// <summary>
        /// FIXME
        /// </summary>
        Task<string> TryAcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);

        /// <summary>
        /// FIXME
        /// </summary>
        /// <param name="leaseDefinition"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<string> AcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);

        /// <summary>
        /// FIXME
        /// </summary>
        Task RenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);

        /// <summary>
        /// FIXME
        /// </summary>
        Task ReleaseLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);

        /// <summary>
        /// FIXME
        /// </summary>
        Task WriteLeaseBlobMetadataAsync(LeaseDefinition leaseDefinition, string key, string value, CancellationToken cancellationToken);

        /// <summary>
        /// FIXME
        /// </summary>
        Task<LeaseInformation> ReadLeaseInfoAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken);
    }
}
