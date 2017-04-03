// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Lease;
using Newtonsoft.Json;

// FIXME: Caching
// FIXME: fix this whole file
namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// FIXME
    /// </summary>
    internal class SqlLeasor : ILeasor
    {
        private static readonly string InstanceId = Guid.NewGuid().ToString();

        public static bool IsSqlLeaseType()
        {
            bool isSqlLeaseType;
            try
            {
                string connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(ConnectionStringNames.Leasor);
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                }

                isSqlLeaseType = true;
            }
            catch (Exception)
            {
                isSqlLeaseType = false;
            }

            return isSqlLeaseType;
        }

        /// <summary>
        /// FIXME
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
        /// FIXME
        /// </summary>
        /// <param name="leaseDefinition"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<string> AcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            bool isAcquired;
            try
            {
                isAcquired = await AcquireOrRenewLeaseAsync(leaseDefinition, cancellationToken);
            }
            catch (Exception ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }

            if (isAcquired)
            {
                return "FIXME: make sure the return value here and renew's params are in sync";
            }

            throw new LeaseException(LeaseFailureReason.Conflict, null);
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public async Task RenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            bool isAcquired;
            try
            {
                isAcquired = await AcquireOrRenewLeaseAsync(leaseDefinition, cancellationToken);
            }
            catch (Exception ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }

            if (!isAcquired)
            {
                throw new LeaseException(LeaseFailureReason.Conflict, null);
            }
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public Task WriteLeaseBlobMetadataAsync(LeaseDefinition leaseDefinition, string key,
            string value, CancellationToken cancellationToken)
        {
            var metadata = new Dictionary<string, string>
            {
                {key, value}
            };

            var connectionString = GetConnectionString(leaseDefinition.AccountName);
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

            return Task.CompletedTask;
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public Task<LeaseInformation> ReadLeaseInfoAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            var connectionString = GetConnectionString(leaseDefinition.AccountName);
            string serializedMetadata = null;

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
        /// FIXME
        /// </summary>
        public Task ReleaseLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            var connectionString = GetConnectionString(leaseDefinition.AccountName);
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

            return Task.CompletedTask;
        }

        /// <summary>
        /// FIXME
        /// </summary>
        //public static bool TryCreateAsync(out ILeasor leasor) // FIXME: do this the very first time
        //{
        //    //leasor = new SqlLeasor();
        //    //return true;

        //    string connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(ConnectionStringNames.Leasor);
        //    if (string.IsNullOrWhiteSpace(connectionString))
        //        return false;

        //    // Validate credentials

        //    try
        //    {
        //        using (SqlConnection connection = new SqlConnection(connectionString))
        //        {
        //            connection.Open();
        //        }
        //    }
        //    catch (Exception)
        //    {
        //        // Assume that the failure is because the connection string is not a SQL connection string
        //        return false;
        //    }

        //    return true;
        //}
        // FIXME: Verify logging across all files

        private static string GetConnectionString(string accountName) // FIXME: revamp how account name and connection strings are handled for both blobleasor and sqlleasor, also for leasorfactory
        {
            // FIXME
            string connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(accountName);

            return connectionString;
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

        private string GetLeaseName(LeaseDefinition leaseDefinition)
        {
            // Also make sure that none of these have a pipe character in them. FIXME.
            return string.Format("{0}|{1}|{2}", leaseDefinition.Namespace, leaseDefinition.Category, leaseDefinition.LockId);
        }
    }
}
