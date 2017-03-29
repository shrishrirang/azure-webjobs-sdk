// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host.Lease;

// FIXME: fix this whole file
namespace Microsoft.Azure.WebJobs.Host.Lease
{
    /// <summary>
    /// FIXME
    /// </summary>
    internal class SqlLeasor : ILeasor
    {
        private static readonly string InstanceId = Guid.NewGuid().ToString();

        /// <summary>
        /// FIXME
        /// </summary>
        public SqlLeasor()
        {
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
                if (string.IsNullOrEmpty(leaseDefinition.LockId))
                {
                    leaseDefinition.LockId = Guid.NewGuid().ToString(); // FIXME: is this a scenario we want to support?
                }

                isAcquired = await AcquireOrRenewLeaseAsync(leaseDefinition, cancellationToken);
            }
            catch (Exception ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }

            if (isAcquired)
            {
                return leaseDefinition.LockId;
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
                leaseDefinition.LockId = leaseDefinition.LeaseId; // FIXME: need a better way to  handle this
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
            throw new NotImplementedException();
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public Task<LeaseInformation> ReadLeaseInfoAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public async Task ReleaseLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString(leaseDefinition.AccountName)))
            {
                await connection.OpenAsync();
                using (SqlCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "[runtime].leases_release";
                    cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseName(leaseDefinition);
                    cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId;
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        /// <summary>
        /// FIXME
        /// </summary>
        public static bool TryGetAccountAsync(string accountName, out ILeasor leasor)
        {
            leasor = new SqlLeasor();
            return true;
            //var connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(accountName);
            //if (string.IsNullOrWhiteSpace(connectionString))
            //    return false; // FIXME: identify if this is a sql connection string

            //return false;
        }

        private string GetConnectionString(string accountName) // FIXME: revamp how account name and connection strings are handled for both blobleasor and sqlleasor, also for leasorfactory
        {
            //return AmbientConnectionStringProvider.Instance.GetConnectionString(accountName);
            string connectionString = "FIXME";
            
            return connectionString;
        }

        private async Task<bool> AcquireOrRenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            var connectionString = GetConnectionString(leaseDefinition.AccountName);

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (SqlCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "[runtime].leases_tryAcquireOrRenew";
                    cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseName(leaseDefinition);
                    cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId; // FIXME: is this used to decide whether to allow re-acquiring?
                    cmd.Parameters.Add("@Metadata", SqlDbType.NVarChar).Value = "meta";
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
