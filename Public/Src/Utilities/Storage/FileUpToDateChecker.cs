// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics.ContractsLight;
using BuildXL.Native.IO;
using BuildXL.Storage.ChangeJournalService;
using BuildXL.Storage.ChangeTracking;
using BuildXL.Utilities;
using BuildXL.Utilities.Collections;

namespace BuildXL.Storage
{
    /// <summary>
    /// Class representing state of journal.
    /// </summary>
    public class FileUpToDateChecker
    {
        private ConcurrentBigMap<AbsolutePath, DirectoryEntry> m_directoryEntries;
        private ConcurrentBigMap<FileId, AbsolutePath> m_idToPathMap;

        private ConcurrentBigMap<AbsolutePath, FileId> pathToFileIdMapping;
        private ConcurrentBigMap<(FileId parentId, StringId name), FileSystemEntry> fsHierarchy;
        private ConcurrentBigMap<FileId, FileSystemEntry> fileContentTable;

        private IChangeJournalAccessor m_journal;

        // Changes

        private void OnContentChange(FileId fileId, Usn usn)
        {
            if (fileContentTable.TryGetValue(fileId, out var entry))
            {
                entry.ContentInfo = default;
            }
        }

        private void OnMemberChange(FileId parentId, FileId memberId, StringId name, MembershipImpact impact)
        {
            fsHierarchy.TryGetValue((parentId, name), out var entry);
            if (impact == MembershipImpact.Deletion)
            {
                fsHierarchy.RemoveKey((parentId, name));
            }
            else if (impact == MembershipImpact.Creation)
            {
                fsHierarchy[(parentId, name)] = new FileEntry()
                {
                    Id = memberId,
                    // Set fields
                };
            }
        }

        // TODO:
        // Track certain paths globally (i.e. Maybe non-standard drives which are known to be probed and probably never exist or
        // if they do exist, they don't contain relevant paths)
        // SourceCache/source control service should request source control paths to be tracked.
        // Adding junction/dir symlink causes interesting effect since it adds a directory tree.
        // Build should requests that certain paths be tracked.

        // Check parent tree to see if file still exists at path
        
        // Send cache lookup to worker with most up to date FileUpToDateChecker

        // Query cache to get which file ids it owns? The files are read-only so they cannot be modified

        // Capture the sequence number when the step if first enqueued
        public SequenceNumber GetSequenceNumber()
        {
            throw new NotImplementedException();
        }

        public bool TryGetFileIdForPath(AbsolutePath path, out FileId fileId)
        {
            throw new NotImplementedException();
        }

        private bool TryGetEntryForPath(AbsolutePath path, out FileSystemEntry entry)
        {
            if (TryGetFileIdForPath(path, out var fileId) && fileContentTable.TryGetValue(fileId, out entry))
            {
                return true;
            }

            entry = default;
            return false;
        }

        public bool TryGetPathExistence(AbsolutePath path, SequenceNumber sequenceNumber, out PathExistence existence)
        {
            // Check nearest existing containing directory
            if (!EnsureSequenceNumber(sequenceNumber))
            {
                existence = default;
                return false;
            }

            if (TryGetEntryForPath(path, out var entry))
            {
                existence = entry.Existence;
                return true;
            }

            // Walk up parent path? If parent path is tracked, then assume path does not exist.

            existence = default;
            return false;
        }

        public bool TryGetFileContent(AbsolutePath path, SequenceNumber sequenceNumber, out FileContentInfo contentInfo)
        {
            if (!EnsureSequenceNumber(sequenceNumber))
            {
                contentInfo = default;
                return false;
            }

            if (TryGetEntryForPath(path, out var entry) && entry.HasContentInfo)
            {
                contentInfo = entry.ContentInfo;
                return contentInfo.IsValid;
            }

            contentInfo = default;
            return false;
        }

        private bool EnsureSequenceNumber(SequenceNumber sequenceNumber)
        {

        }

        private bool IsValid(FileContentInfo contentInfo)
        {
            throw new NotImplementedException();
        }

        private class FileSystemEntry
        {
            public bool HasContentInfo;
            public DateTime LastWriteTimeUtc;
            public PathExistence Existence;
            public FileContentInfo ContentInfo;
            public FileId Id;
            public AbsolutePath? Path;
            public Usn Usn;
            public int PreviousSibling;
            public int NextSibling;
            public int Version;
        }

        public struct SequenceNumber
        {
        }
    }
}
