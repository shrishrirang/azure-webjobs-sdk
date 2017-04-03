// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Lease;
using Microsoft.Azure.WebJobs.Host.Storage;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// FIXME
    /// </summary>
    internal class LeasorFactory
    {
        // FIXME: what if no storage connection strings are defined? that should be supported too. Introduce InMemoryLeasor that implements ILeasor.cs
        /// <summary>
        /// FIXME
        /// </summary>
        public static ILeasor CreateLeasor(IStorageAccountProvider storageAccountProvider)
        {
            ILeasor leasor = null;

            string leasorType = ConfigurationUtility.GetSettingFromConfigOrEnvironment(Constants.LeasorTypeSettingName);

            if (string.Equals(leasorType, Constants.SqlLeasorType, StringComparison.OrdinalIgnoreCase))
            {
                leasor = new SqlLeasor();
            }
            else
            {
                leasor = new BlobLeasor(storageAccountProvider);
            }

            return leasor;
        }
    }
}
