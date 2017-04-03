// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// FIXME
    /// </summary>
    public class LeaseDefinition
    {
        /// <summary>
        /// FIXME
        /// </summary>
        public string AccountName { get; set; } // FIXME: why is this accountname and not connection string?

        /// <summary>
        /// FIXME
        /// </summary>
        public string Namespace { get; set; }

        /// <summary>
        /// FIXME
        /// </summary>
        public string Category { get; set; } // FIXME: change naming of namespace category lockid to container directory blobname

        /// <summary>
        /// FIXME
        /// </summary>
        public string LockId { get; set; }

        /// <summary>
        /// FIXME
        /// </summary>
        public string LeaseId { get; set; }

        /// <summary>
        /// FIXME
        /// </summary>
        public TimeSpan Period { get; set; }
    }
}
