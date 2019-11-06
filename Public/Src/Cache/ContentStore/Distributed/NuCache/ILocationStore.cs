// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BuildXL.Cache.ContentStore.Hashing;
using BuildXL.Cache.ContentStore.Interfaces.Results;
using BuildXL.Cache.ContentStore.Interfaces.Stores;
using BuildXL.Cache.ContentStore.Tracing.Internal;
using BuildXL.Cache.ContentStore.UtilitiesCore;
using BuildXL.Utilities.Tracing;

namespace BuildXL.Cache.ContentStore.Distributed.NuCache
{
    /// <summary>
    /// Interface that represents a location store for tracking distributed content
    /// </summary>
    public interface ILocationStore : IStartupShutdownSlim
    {
        /// <summary>
        /// Gets the list of <see cref="ContentLocationEntry"/> for every hash specified by <paramref name="contentHashes"/> from a central store.
        /// </summary>
        /// <remarks>
        /// The resulting collection (in success case) will have the same size as <paramref name="contentHashes"/>.
        /// </remarks>
        Task<Result<IReadOnlyList<ContentLocationEntry>>> GetBulkAsync(OperationContext context, IReadOnlyList<ContentHash> contentHashes, GetBulkOrigin origin);

        /// <summary>
        /// Notifies a central store about the locations represented the given entries.
        /// </summary>
        Task<BoolResult> RegisterEntries(OperationContext context, IReadOnlyList<(ShortHash hash, ContentLocationEntry entry)> entries);

        /// <summary>
        /// Notifies a central store that content represented by <paramref name="contentHashes"/> is available on a given machine.
        /// </summary>
        Task<BoolResult> RegisterLocationAsync(OperationContext context, MachineId location, IReadOnlyList<ContentHashWithSize> contentHashes);

        /// <summary>
        /// Puts a blob into the content location store.
        /// </summary>
        Task<BoolResult> PutBlobAsync(OperationContext context, ContentHash hash, ArraySegment<byte> blob);

        /// <summary>
        /// Gets a blob from the content location store.
        /// </summary>
        Task<Result<byte[]>> GetBlobAsync(OperationContext context, ContentHash hash);

        /// <summary>
        /// Gets a value indicating whether the store supports storing and retrieving blobs.
        /// </summary>
        bool AreBlobsSupported { get; }

        /// <nodoc />
        CounterSet GetCounters(OperationContext context);
    }
}
