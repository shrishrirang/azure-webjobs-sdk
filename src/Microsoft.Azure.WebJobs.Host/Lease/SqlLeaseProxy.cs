// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

// TOOD: Renew, Release, etc should be able to work off of Lease ID just like the blob implementation does
// The need to specify account account name, namespaces, etc is unnecessary for anything other than lease acquiring
// For everything else, just the Lease ID should suffice.

namespace Microsoft.Azure.WebJobs.Host.Lease
{
    // Sql based lease implementation
    internal class SqlLeaseProxy : ILeaseProxy
    {
        // TODO: Avoid using a process wide InstanceId.
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

        /// <inheritdoc />
        public async Task<string> TryAcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            if (leaseDefinition == null)
            {
                throw new ArgumentNullException(nameof(leaseDefinition));
            }

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

        /// <inheritdoc />
        public async Task<string> AcquireLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            if (leaseDefinition == null)
            {
                throw new ArgumentNullException(nameof(leaseDefinition));
            }

            try
            {
                bool isAcquired = await TryAcquireOrRenewLeaseAsync(leaseDefinition, cancellationToken);

                if (isAcquired)
                {
                    return GetLeaseId(leaseDefinition);
                }

                throw new LeaseException(LeaseFailureReason.Conflict, null);
            }
            catch (LeaseException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }
        }

        /// <inheritdoc />
        public async Task RenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            if (leaseDefinition == null)
            {
                throw new ArgumentNullException(nameof(leaseDefinition));
            }

            try
            {
                // TODO: Update implementation to use LeaseDefinition.LeaseId for lease renewal
                bool isAcquired = await TryAcquireOrRenewLeaseAsync(leaseDefinition, cancellationToken);

                if (!isAcquired)
                {
                    throw new LeaseException(LeaseFailureReason.Conflict, null);
                }
            }
            catch (LeaseException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }
        }

        /// <inheritdoc />
        public async Task WriteLeaseMetadataAsync(LeaseDefinition leaseDefinition, string key,
            string value, CancellationToken cancellationToken)
        {
            if (leaseDefinition == null)
            {
                throw new ArgumentNullException(nameof(leaseDefinition));
            }

            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            try
            {
                var metadata = new Dictionary<string, string> { { key, value } };

                var connectionString = GetConnectionString(leaseDefinition.AccountName);

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync(cancellationToken);

                    using (SqlCommand cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = "[function].[leases_updateMetadata]";
                        cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseId(leaseDefinition);
                        cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId;
                        cmd.Parameters.Add("@Metadata", SqlDbType.NVarChar).Value = JsonConvert.SerializeObject(metadata);
                        cmd.Parameters.Add("@Successful", SqlDbType.Bit).Direction = ParameterDirection.Output;

                        await cmd.ExecuteNonQueryAsync(cancellationToken);

                        var successful = (bool)cmd.Parameters["@Successful"].Value;

                        // Assume failure is due to expired lease. Technically, we might have failed to set the metadata because 
                        // the db contains no entry for the specified lease name. If we need to be accurate about the error,
                        // Update the stored proc to provide detailed status values instead of just a bool. For now, keeping it simple
                        if (!successful)
                        {
                            throw new LeaseException(LeaseFailureReason.Conflict, null);
                        }
                    }
                }
            }
            catch (LeaseException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }
        }

        /// <inheritdoc />
        public async Task<LeaseInformation> ReadLeaseInfoAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            if (leaseDefinition == null)
            {
                throw new ArgumentNullException(nameof(leaseDefinition));
            }

            try
            {
                var connectionString = GetConnectionString(leaseDefinition.AccountName);

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync(cancellationToken);

                    using (SqlCommand cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = "[function].leases_getMetadata";
                        cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseId(leaseDefinition);
                        cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId;
                        cmd.Parameters.Add("@Metadata", SqlDbType.NVarChar, -1).Direction = ParameterDirection.Output;
                        cmd.Parameters.Add("@HasLease", SqlDbType.Bit).Direction = ParameterDirection.Output;
                        await cmd.ExecuteNonQueryAsync(cancellationToken);

                        var hasLease = (bool)cmd.Parameters["@HasLease"].Value;

                        var serializedMetadata = (string)cmd.Parameters["@Metadata"].Value;

                        Dictionary<string, string> metadataDict;

                        if (string.IsNullOrEmpty(serializedMetadata))
                        {
                            metadataDict = new Dictionary<string, string>();
                        }
                        else
                        {
                            metadataDict = JsonConvert.DeserializeObject<Dictionary<string, string>>(serializedMetadata);
                        }

                        // We don't want to fail even if the lease is not active
                        return new LeaseInformation(hasLease, metadataDict);
                    }
                }
            }
            catch (SqlException ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }
        }

        /// <inheritdoc />
        public async Task ReleaseLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            if (leaseDefinition == null)
            {
                throw new ArgumentNullException(nameof(leaseDefinition));
            }

            try
            {
                var connectionString = GetConnectionString(leaseDefinition.AccountName);

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync(cancellationToken);

                    using (SqlCommand cmd = connection.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandText = "[function].[leases_release]";
                        cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseId(leaseDefinition);
                        cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId;

                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new LeaseException(LeaseFailureReason.Unknown, ex);
            }
        }

        // TODO: LeaseDefinition.LeaseId should be used as the proposed LeaseId if it is defined
        private static async Task<bool> TryAcquireOrRenewLeaseAsync(LeaseDefinition leaseDefinition, CancellationToken cancellationToken)
        {
            var connectionString = GetConnectionString(leaseDefinition.AccountName);

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync(cancellationToken);
                using (SqlCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = "[function].[leases_tryAcquireOrRenew]";
                    cmd.Parameters.Add("@LeaseName", SqlDbType.NVarChar, 127).Value = GetLeaseId(leaseDefinition);
                    cmd.Parameters.Add("@RequestorName", SqlDbType.NVarChar, 127).Value = InstanceId;
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

            leaseId += "&" + WebUtility.UrlEncode(leaseDefinition.Name);

            return leaseId;
        }

        private static string GetConnectionString(string accountName)
        {
            if (string.IsNullOrWhiteSpace(accountName))
            {
                throw new InvalidOperationException("Lease account name not specified");
            }

            return AmbientConnectionStringProvider.Instance.GetConnectionString(accountName);
        }
    }
}
