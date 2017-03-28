// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// FIXME
    /// </summary>
    public class LeaseException : Exception // FIXME: dos and donts of extending exceptions
    {
        /// <summary>
        /// FIXME
        /// </summary>
        /// <param name="failureReason"></param>
        /// <param name="innerException"></param>
        public LeaseException(LeaseFailureReason failureReason, Exception innerException)
            : base(null, innerException)
        {
            FailureReason = failureReason;
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public LeaseFailureReason FailureReason { get; protected set; }

        // FIXME: do we need to implement tostring?
    }
}
