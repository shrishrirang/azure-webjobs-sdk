// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Data;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// FIXME
    /// </summary>
    public class LeaseInformation // FIXME: review and mark classes as internal
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public LeaseInformation(bool isLeaseAvailable, IDictionary<string, string> metadata)
        {
            this.IsLeaseAvailable = isLeaseAvailable;
            this.Metadata = metadata;
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public bool IsLeaseAvailable { get; set; }

        /// <summary>
        /// FIXME
        /// </summary>
        public IDictionary<string, string> Metadata { get; private set; }
    }
}
