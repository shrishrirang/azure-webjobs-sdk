// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Lease;
using Newtonsoft.Json;

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    // Sql based lease implementation
    internal class SqlLeaseProxy : ILeaseProxy
    {
        // <Lease namespaces, Lease Name, InstanceID> together form the primary key for the SQL based lease implementation.
        // This means, multiple attempts to acquire lease on the same object will succeed, if they are performed in the same process.
        // If this limitation is not acceptable, consider generating a unique ID every time an attempt is made to acquire the lease.
        private static readonly string InstanceId = Guid.NewGuid().ToString();

        public static bool IsSqlLeaseType()
        {
            try
            {
                string connectionString = GetConnectionString(ConnectionStringNames.Lease);

                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    return false;
                }

                // Try creating a SQL connection. This will implicitly parse the connection string
                // and throw an exception if it fails.
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                }

                return true;
            }
            catch (Exception)
            {
            }

            return false;
        }

        /// <summary>
        /// <see cref="ILeaseProxy.TryAcquireLeaseAsync"/>
        /// </summary>
        public async Task<string> TryAcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            string leaseId = null;
            try
            {
                leaseId = await AcquireLeaseAsync(leaseDefinition, cancellationToken);
            }
            catch (Exception)
            {
            }

            return leaseId;
        }

        /// <summary>
        /// <see cref="ILeaseProxy.AcquireLeaseAsync"/>
        /// </summary>
        public async Task<string> AcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            bool isAcquired;
            try
            {
                isAcquired = await AcquireOrRenewLeaseAsync(leaseDefinition, cancellationToken);
            }
            catch (SqlException ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }

            if (isAcquired)
            {
                // We don't need a lease ID for renewing / releasing sql based leases. 
                // Simply return the lease name as the lease ID to adhere to the interface definition.
                return GetLeaseId(leaseDefinition);
            }

            throw new LeaseException(LeaseFailureReason.Conflict, null);
        }

        /// <summary>
        /// <see cref="ILeaseProxy.RenewLeaseAsync"/>
        /// </summary>
        public async Task RenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            bool isAcquired;
            try
            {
                isAcquired = await AcquireOrRenewLeaseAsync(leaseDefinition, cancellationToken);
            }
            catch (SqlException ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }

            if (!isAcquired)
            {
                throw new LeaseException(LeaseFailureReason.Conflict, null);
            }
        }

        /// <summary>
        /// <see cref="ILeaseProxy.WriteLeaseMetadataAsync"/>
        /// </summary>
        public Task WriteLeaseMetadataAsync(LeaseDefinition leaseDefinition, string key,
            string value, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// <see cref="ILeaseProxy.ReadLeaseInfoAsync"/>
        /// </summary>
        public Task<LeaseInformation> ReadLeaseInfoAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// <see cref="ILeaseProxy.ReleaseLeaseAsync"/>
        /// </summary>
        public Task ReleaseLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            if (leaseDefinition == null)
            {
                throw new ArgumentNullException(nameof(leaseDefinition));
            }

            var connectionString = GetConnectionString(leaseDefinition.AccountName);
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (SqlCommand cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = "[functions].leases_release";
                        cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseId(leaseDefinition);
                        cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (SqlException ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }

            return Task.CompletedTask;
        }

        private static string GetConnectionString(string accountName)
        {
            if (string.IsNullOrWhiteSpace(accountName))
            {
                throw new InvalidOperationException("Lease account name not specified");
            }

            return AmbientConnectionStringProvider.Instance.GetConnectionString(accountName);
        }

        private async Task<bool> AcquireOrRenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            var connectionString = GetConnectionString(leaseDefinition.AccountName);

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (SqlCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "[functions].leases_tryAcquireOrRenew";
                    cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseId(leaseDefinition);
                    cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId;
                    cmd.Parameters.Add("@Metadata", SqlDbType.NVarChar).Value = JsonConvert.SerializeObject(new Dictionary<string, string>());
                    cmd.Parameters.Add("@LeaseExpirationTimeSpan", SqlDbType.Int).Value = leaseDefinition.Period.TotalSeconds;
                    cmd.Parameters.Add("@HasLease", SqlDbType.Bit).Direction = ParameterDirection.Output;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);

                    return (bool)cmd.Parameters["@HasLease"].Value;
                }
            }
        }

        private static string GetLeaseId(LeaseDefinition leaseDefinition)
        {
            if (leaseDefinition.Namespaces == null || leaseDefinition.Namespaces.Count < 1 || leaseDefinition.Namespaces.Count > 2)
            {
                throw new InvalidOperationException(String.Format(CultureInfo.InvariantCulture, "Invalid LeaseDefinition Namespaces: {0}", leaseDefinition.Namespaces));
            }

            string leaseId = WebUtility.UrlEncode(leaseDefinition.Namespaces[0]);

            if (leaseDefinition.Namespaces.Count == 2)
            {
                leaseId += "&" + WebUtility.UrlEncode(leaseDefinition.Namespaces[1]);
            }

            return leaseId;
        }
    }
}
