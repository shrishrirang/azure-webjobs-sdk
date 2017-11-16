// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Host
{
    // File baesd distributed lock manager
    internal class FileDistributedLockManager : IDistributedLockManager
    {
        public async Task<IDistributedLock> TryLockAsync(string account, string lockId, string lockOwnerId,
            string proposedLeaseId, TimeSpan lockPeriod, CancellationToken cancellationToken)
        {
            LockFileInfo activeLockFile = GetActiveLockFile(account, lockId);

            if (activeLockFile == null)
            {
                var lockFilePath = CreateLockFile(account, lockId, lockPeriod, lockOwnerId, proposedLeaseId);

                if (await TryAcquireOrRenewLockAsync(account, lockId, lockFilePath))
                {
                    return new FileLockHandle(account, lockId, lockPeriod, lockFilePath);
                }

                File.Delete(lockFilePath);
            }

            return null;
        }

        public async Task<bool> RenewAsync(IDistributedLock lockHandle, CancellationToken cancellationToken)
        {
            var handle = lockHandle as FileLockHandle;

            if (handle == null)
            {
                return false;
            }

            // Update the lock file's timestamp. We will worry about whether renewal can be successfully performed or not in the next step
            TouchLockFile(handle.LockFilePath);

            var renewalSucceeded = await TryAcquireOrRenewLockAsync(handle.Account, handle.LockId, handle.LockFilePath);

            if (!renewalSucceeded)
            {
                // If renewal failed, we don't need the lock file anymore
                File.Delete(handle.LockFilePath);
            }

            return renewalSucceeded;
        }

        public Task<string> GetLockOwnerAsync(string account, string lockId, CancellationToken cancellationToken)
        {
            LockFileInfo activeLockFile = GetActiveLockFile(account, lockId);

            if (activeLockFile == null)
            {
                return null;
            }

            return Task.FromResult(activeLockFile.Owner);
        }

        public Task ReleaseLockAsync(IDistributedLock lockHandle, CancellationToken cancellationToken)
        {
            var handle = lockHandle as FileLockHandle;

            if (handle != null)
            {
                // Done with the lock file, delete it
                File.Delete(handle.LockFilePath);
            }

            return Task.CompletedTask;
        }

        private static Task<bool> TryAcquireOrRenewLockAsync(string account, string lockId, string lockFilePath)
        {
            var activeLockFile1 = GetActiveLockFile(account, lockId);

            // If active lock file is the same as the current lock file, acquiring / renewing the lock should be fine
            return Task.FromResult(activeLockFile1 != null && activeLockFile1.FilePath.Equals(lockFilePath));
        }

        [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "context")]
        private static string GetFileShare(string account)
        {
            // FIXME
            return Path.Combine(Environment.GetEnvironmentVariable("HOME"), "lease", account);
        }

        private static string GetLockDirectoryPath(string account, string lockId)
        {
            return Path.Combine(GetFileShare(account), lockId);
        }

        // TODO: Need to remove stale files, periodically
        private static string CreateLockFile(string account, string lockId, TimeSpan lockPeriod, string lockOwnerId, string proposedLeaseId)
        {
            var dir = GetLockDirectoryPath(account, lockId);

            // TODO: For now, override the proposedLeaseId to avoid worrying about lease name conflict handling
            proposedLeaseId = Guid.NewGuid() + ".txt";

            var lockFilePath = Path.Combine(dir, proposedLeaseId);

            Dictionary<string, object> lockFileContent = new Dictionary<string, object>
            {
                { "duration", lockPeriod.TotalSeconds },
                { "owner", lockOwnerId }
            };

            File.WriteAllText(lockFilePath, JsonConvert.SerializeObject(lockFileContent));

            return lockFilePath;
        }

        private static void TouchLockFile(string lockFilePath)
        {
            File.AppendAllText(lockFilePath, " ");
        }

        // Get the lock file that corresponds to the instance that has acquired the lock
        private static LockFileInfo GetActiveLockFile(string account, string lockId)
        {
            var lockFiles = GetLockInfo(account, lockId);

            LockFileInfo activeLockFile = null;

            foreach (var lockFile in lockFiles)
            {
                // If the lock associated with the file has expired, it is just a stale file we are not interested in
                if (lockFile.ExpiryTimeUtc <= GetCurrentServerTimeUtc())
                {
                    continue;
                }

                // Files with earlier timestamp override the ones with a later timestamp
                if (activeLockFile == null || lockFile.LastWriteTimeUtc < activeLockFile.LastWriteTimeUtc)
                {
                    activeLockFile = lockFile;
                    continue;
                }

                // In the rare event that two lock files are modified at the same time, we use alphabetical ordering to decide who gets to hold the lock
                if (lockFile.LastWriteTimeUtc == activeLockFile.LastWriteTimeUtc &&
                    String.Compare(lockFile.FilePath, activeLockFile.FilePath, StringComparison.Ordinal) < 0)
                {
                    activeLockFile = lockFile;
                }
            }

            return activeLockFile;
        }

        // TODO: For now we assume that the server and client clocks are in sync. 
        // However, The clocks can be out of sync. A trick to work around this could be to create a file on server
        // and use it's creation timestamp as a hint.
        private static DateTime GetCurrentServerTimeUtc()
        {
            return DateTime.UtcNow;
        }

        private static List<LockFileInfo> GetLockInfo(string account, string lockId)
        {
            var lockDirectory = GetLockDirectoryPath(account, lockId);

            Directory.CreateDirectory(lockDirectory);

            var files = Directory.GetFiles(lockDirectory);

            var lockFiles = new List<LockFileInfo>();

            // TODO: This will be very slow if number of files is large OR if access to the file share is slow
            foreach (var file in files)
            {
                var info = GetLockFileInfo(file);
                if (info != null)
                {
                    lockFiles.Add(info);
                }
            }

            return lockFiles;
        }

        private static LockFileInfo GetLockFileInfo(string lockFilePath)
        {
            var lastWriteTimeUtc = File.GetLastWriteTimeUtc(lockFilePath);

            var contentString = File.ReadAllText(lockFilePath);
            var contentDict = (JObject)JsonConvert.DeserializeObject(contentString);

            if (contentDict == null)
            {
                return null;
            }

            return LockFileInfo.Create(lockFilePath, lastWriteTimeUtc, contentDict);
        }

        internal class FileLockHandle : IDistributedLock
        {
            public FileLockHandle(string account, string lockId, TimeSpan lockPeriod, string lockFilePath)
            {
                Account = account;
                LockId = lockId;
                LockPeriod = lockPeriod;
                LockFilePath = lockFilePath;
            }

            public string Account { get; private set; }

            public string LockId { get; private set; }

            public TimeSpan LockPeriod { get; private set; }

            public string LockFilePath { get; private set; }
        }

        // Info about the lock file associated with the client's lock 
        internal class LockFileInfo
        {
            private LockFileInfo()
            {
            }

            public string FilePath { get; private set; }

            public DateTime LastWriteTimeUtc { get; private set; }

            public int LockDuration { get; private set; }

            public string Owner { get; private set; }

            public DateTime ExpiryTimeUtc => LastWriteTimeUtc.AddSeconds(LockDuration);

            public static LockFileInfo Create(string filePath, DateTime lastWriteTimeUtc, JObject lockFileContent)
            {
                return new LockFileInfo
                {
                    FilePath = filePath,
                    LastWriteTimeUtc = lastWriteTimeUtc,
                    LockDuration = (int)lockFileContent["duration"],
                    Owner = (string)lockFileContent["owner"],
                };
            }
        }
    }
}