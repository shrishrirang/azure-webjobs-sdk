// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// A Lease Exception
    /// </summary>
    public class LeaseException : Exception // FIXME: dos and donts of extending exceptions
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public LeaseException(LeaseFailureReason failureReason, Exception innerException)
            : base(null, innerException)
        {
            FailureReason = failureReason;
        }

        /// <summary>
        /// Lease failure reason
        /// </summary>
        public LeaseFailureReason FailureReason { get; protected set; }
    }
}
