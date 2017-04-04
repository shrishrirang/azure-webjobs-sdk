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
        public string AccountName { get; set; }

        /// <summary>
        /// List of nested logical namespaces that will contain the lease
        /// </summary>
        public IReadOnlyList<string> Namespaces { get; set; }

        /// <summary>
        /// The lease name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The lease ID
        /// </summary>
        public string LeaseId { get; set; }

        /// <summary>
        /// Duration of the lease
        /// </summary>
        public TimeSpan Period { get; set; }
    }
}
