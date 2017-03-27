// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    internal class LeaseDefinition
    {
        public string AccountName { get; set; }

        public string Namespace { get; set; }

        public string Category { get; set; }

        public string Id { get; set; }

        public TimeSpan Period { get; set; }
    }
}
