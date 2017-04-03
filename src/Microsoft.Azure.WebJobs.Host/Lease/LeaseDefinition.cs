// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// Lease definition
    /// </summary>
    public class LeaseDefinition
    {
        /// <summary>
        /// Account name associated with this lease
        /// </summary>
        public string AccountName { get; set; } // FIXME: why is this accountname and not connection string?

        /// <summary>
        /// FIXME
        /// </summary>
        public IList<string> Namespaces { get; set; }

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
        public string Name { get; set; }

        /// <summary>
        /// FIXME
        /// </summary>
        public string LeaseId { get; set; }

        /// <summary>
        /// Duration of the lease
        /// </summary>
        public TimeSpan Period { get; set; }
    }
}
