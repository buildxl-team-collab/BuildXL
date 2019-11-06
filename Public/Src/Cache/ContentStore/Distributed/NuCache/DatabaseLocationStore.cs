// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BuildXL.Cache.ContentStore.Hashing;
using BuildXL.Cache.ContentStore.Interfaces.Results;
using BuildXL.Cache.ContentStore.Interfaces.Stores;
using BuildXL.Cache.ContentStore.Tracing;
using BuildXL.Cache.ContentStore.Tracing.Internal;
using BuildXL.Cache.ContentStore.UtilitiesCore;
using BuildXL.Cache.ContentStore.Utils;
using BuildXL.Utilities.Collections;
using BuildXL.Utilities.Tracing;

namespace BuildXL.Cache.ContentStore.Distributed.NuCache
{
    /// <summary>
    /// Interface that represents a location store for tracking distributed content
    /// </summary>
    public class DistributedSessionLocationStore : StartupShutdownSlimBase, ILocationStore
    {
        private RocksDbContentLocationDatabase _database;
        private LocalLocationStore _lls;

        public bool AreBlobsSupported => throw new System.NotImplementedException();

        protected override Tracer Tracer => throw new System.NotImplementedException();

        public CounterSet GetCounters(OperationContext context)
        {
            throw new System.NotImplementedException();
        }

        public Task<Result<IReadOnlyList<ContentLocationEntry>>> GetBulkAsync(OperationContext context, IReadOnlyList<ContentHash> contentHashes, GetBulkOrigin origin)
        {
            return context.PerformOperationAsync(
                Tracer,
                async () =>
                {
                    IReadOnlyList<ContentLocationEntry> results = null;
                    if (origin == GetBulkOrigin.Global)
                    {
                        // WIP: Can we avoid querying global if we have queried before?
                        results = await _lls.GlobalStore.GetBulkAsync(context, contentHashes, origin).ThrowIfFailureAsync();
                    }
                    else if (origin == GetBulkOrigin.Local)
                    {
                        results = GetBulk(context, _lls.Database, contentHashes.SelectList(hash => new ShortHash(hash)), results, updateDatabase: false).ThrowIfFailure();
                    }

                    results = GetBulk(context, _database, contentHashes.SelectList(hash => new ShortHash(hash)), results, updateDatabase: true).ThrowIfFailure();

                    return Result.Success(results);
                });
        }

        private Result<IReadOnlyList<ContentLocationEntry>> GetBulk(
            OperationContext context,
            ContentLocationDatabase database,
            IReadOnlyList<ShortHash> contentHashes,
            IReadOnlyList<ContentLocationEntry> entriesToMerge,
            bool updateDatabase)
        {
            // WIP: Logging?

            var entries = new List<ContentLocationEntry>(contentHashes.Count);

            for (int i = 0; i < contentHashes.Count; i++)
            {
                var hash = contentHashes[i];
                ContentLocationEntry entry;
                if (updateDatabase && entriesToMerge != null)
                {
                    // WIP: Add merge function to database
                    entry = database.AddOrMergeEntry(context, hash, entriesToMerge[i]);
                }
                else
                {
                    if (!_database.TryGetEntry(context, hash, out entry))
                    {
                        // NOTE: Entries missing from the local db are not touched. They referring to content which is no longer
                        // in the system or content which is new and has not been propagated through local db
                        entry = ContentLocationEntry.Missing;
                    }

                    if (entriesToMerge != null)
                    {
                        // WIP: Merge entries just is to consolidate merging logic. Maybe there's a better way
                        entry = _database.MergeEntries(entry, entriesToMerge[i]);
                    }
                }

                entries.Add(entry);
            }

            return Result.Success<IReadOnlyList<ContentLocationEntry>>(entries);
        }

        public Task<Result<byte[]>> GetBlobAsync(OperationContext context, ContentHash hash)
        {
            throw new System.NotImplementedException();
        }

        public Task<BoolResult> PutBlobAsync(OperationContext context, ContentHash hash, ArraySegment<byte> blob)
        {
            throw new System.NotImplementedException();
        }

        public Task<BoolResult> RegisterLocationAsync(OperationContext context, MachineId location, IReadOnlyList<ContentHashWithSize> contentHashes)
        {
            return context.PerformOperationAsync(
                Tracer,
                async () =>
                {
                    // WIP: Do we need to register all the hashes?
                    // WIP: Should we just nagle?
                    await _lls.GlobalStore.RegisterLocationAsync(context, location, contentHashes).ThrowIfFailure();

                    foreach (var item in contentHashes)
                    {
                        _database.LocationAdded(context, item.Hash, location, item.Size, reconciling: false);
                    }

                    return BoolResult.Success;
                });
        }

        public Task<BoolResult> RegisterEntries(OperationContext context, IReadOnlyList<(ShortHash hash, ContentLocationEntry entry)> entries)
        {
            return context.PerformOperationAsync(
                Tracer,
                () =>
                {
                    GetBulk(context, _database, entries.SelectList(t => t.hash), entries.SelectList(t => t.entry), updateDatabase: true).ThrowIfFailure();
                    return BoolResult.SuccessTask;
                });
        }
    }
}
