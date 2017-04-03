// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Lease;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json;

// FIXME: Caching
// FIXME: fix this whole file
namespace Microsoft.Azure.WebJobs.Host.Lease
{
    // Sql based lease implementation
    internal class SqlLeasor : ILeasor
    {
        private static readonly string InstanceId = Guid.NewGuid().ToString();

        public static bool IsSqlLeaseType()
        {
            try
            {
                string connectionString = GetConnectionString(ConnectionStringNames.Leasor);

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
        /// <see cref="ILeasor.TryAcquireLeaseAsync"/>
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
        /// <see cref="ILeasor.AcquireLeaseAsync"/>
        /// </summary>
        public async Task<string> AcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            bool isAcquired;
            try
            {
                isAcquired = await AcquireOrRenewLeaseAsync(leaseDefinition, cancellationToken);
            }
            catch (StorageException ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }

            if (isAcquired)
            {
                // We don't need a lease ID for renewing / releasing leases. 
                // Simply return the lease name as the lease ID to adhere to the interface definition.
                return GetLeaseName(leaseDefinition);
            }

            throw new LeaseException(LeaseFailureReason.Conflict, null);
        }

        /// <summary>
        /// <see cref="ILeasor.RenewLeaseAsync"/>
        /// </summary>
        public async Task RenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            bool isAcquired;
            try
            {
                isAcquired = await AcquireOrRenewLeaseAsync(leaseDefinition, cancellationToken);
            }
            catch (StorageException ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }

            if (!isAcquired)
            {
                throw new LeaseException(LeaseFailureReason.Conflict, null);
            }
        }

        /// <summary>
        /// <see cref="ILeasor.WriteLeaseMetadataAsync"/>
        /// </summary>
        public Task WriteLeaseMetadataAsync(LeaseDefinition leaseDefinition, string key,
            string value, CancellationToken cancellationToken)
        {
            var metadata = new Dictionary<string, string>
            {
                {key, value}
            };

            var connectionString = GetConnectionString(leaseDefinition.AccountName);

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString)) // FIXME: Use CreateSqlConnection and club GetConnectionString and new call together.
                {
                    connection.Open(); // should all calls be async to open and  executenonquery?
                    using (SqlCommand cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = "[runtime].leases_updateMetadata";
                        cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseName(leaseDefinition);
                        cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId;
                        cmd.Parameters.Add("@Metadata", SqlDbType.NVarChar).Value = JsonConvert.SerializeObject(metadata);
                        cmd.Parameters.Add("@HasLease", SqlDbType.Bit).Direction = ParameterDirection.Output;

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

        /// <summary>
        /// <see cref="ILeasor.ReadLeaseInfoAsync"/>
        /// </summary>
        public Task<LeaseInformation> ReadLeaseInfoAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            var connectionString = GetConnectionString(leaseDefinition.AccountName);
            string serializedMetadata = null;

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (SqlCommand cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = "[runtime].leases_getMetadata";
                        cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseName(leaseDefinition);
                        cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId; // FIXME: is this used to decide whether to allow re-acquiring?
                        cmd.Parameters.Add("@LeaseExpirationTimeSpan", SqlDbType.Int).Value = leaseDefinition.Period.TotalSeconds;
                        // cmd.Parameters.Add("@Metadata", SqlDbType.NVarChar).Direction = ParameterDirection.Output;
                        // cmd.ExecuteNonQuery();

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                serializedMetadata = reader["Metadata"] as string;
                            }
                        }
                    } 
                }

            }
            catch (StorageException ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }

            var leaseInformation = new LeaseInformation();
            if (string.IsNullOrEmpty(serializedMetadata))
            {
                leaseInformation.IsLeaseAvailable = false;
            }
            else
            {
                leaseInformation.IsLeaseAvailable = true;
                leaseInformation.Metadata = JsonConvert.DeserializeObject<Dictionary<string, string>>(serializedMetadata);
            }

            return Task.FromResult(leaseInformation);
        }

        /// <summary>
        /// <see cref="ILeasor.ReleaseLeaseAsync"/>
        /// </summary>
        public Task ReleaseLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            var connectionString = GetConnectionString(leaseDefinition.AccountName);
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (SqlCommand cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = "[runtime].leases_release";
                        cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseName(leaseDefinition);
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

        // FIXME: Verify logging across all files
        private static string GetConnectionString(string accountName) // FIXME: revamp how account name and connection strings are handled for both blobleasor and sqlleasor, also for leasorfactory
        {
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
                    cmd.CommandText = "[runtime].leases_tryAcquireOrRenew";
                    cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseName(leaseDefinition);
                    cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId; // FIXME: is this used to decide whether to allow re-acquiring?
                    cmd.Parameters.Add("@Metadata", SqlDbType.NVarChar).Value = JsonConvert.SerializeObject(new Dictionary<string, string>());
                    cmd.Parameters.Add("@LeaseExpirationTimeSpan", SqlDbType.Int).Value = leaseDefinition.Period.TotalSeconds;
                    cmd.Parameters.Add("@HasLease", SqlDbType.Bit).Direction = ParameterDirection.Output;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);

                    return (bool)cmd.Parameters["@HasLease"].Value;
                }
            }
        }

        private static void ValidateLeaseName(LeaseDefinition leaseDefinition)
        {
            if (leaseDefinition.Namespace.Contains("|"))
            {
                throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Invalid lease Namespace: {0}", leaseDefinition.Namespace));
            }

            if (leaseDefinition.Category.Contains("|"))
            {
                throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Invalid lease Category: {0}", leaseDefinition.Category));
            }

            if (leaseDefinition.LockId.Contains("|"))
            {
                throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Invalid lease LockId: {0}", leaseDefinition.LockId));
            }
        }

        private static string GetLeaseName(LeaseDefinition leaseDefinition)
        {
            // Also make sure that none of these have a pipe character in them. FIXME.
            return string.Format(CultureInfo.InvariantCulture, "{0}|{1}|{2}", leaseDefinition.Namespace, leaseDefinition.Category, leaseDefinition.LockId);
        }
    }
}
