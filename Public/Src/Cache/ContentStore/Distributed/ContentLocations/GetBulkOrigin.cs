// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace BuildXL.Cache.ContentStore.Distributed
{
    /// <summary>
    /// Target location for <see cref="GetBulkLocationsResult"/>.
    /// </summary>
    public enum GetBulkOrigin
    {
        /// <summary>
        /// The locations should be obtained from a local store.
        /// </summary>
        Local,

        /// <summary>
        /// Gets locations known to other machines in the build ring
        /// WIP: These locations should not be used for total locations used by pin cache. (Maybe with the
        /// exception of locations in the build ring. Maybe if location is on machine in build ring, we should
        /// just send a pin request to that machine.
        /// 
        /// WIP: Be sure to account for surviving epoch reset (need to handle cluster state reset with new machine mapping)
        /// 
        /// WIP: Origin query ordering might need to be different for different operations.
        /// 
        /// WIP: Query redis via build ring rather than querying directly? Avoid some redis queries. Its possible that when
        /// querying Redis we should only ever query once in a build ring. Subsequent queries should use stored redis results.
        /// 
        /// WIP: How to clean up database? Maybe entry age?
        /// 
        /// WIP: How to preserve cluster state in a build session.
        /// </summary>
        BuildRing,

        /// <summary>
        /// The locations should be obtained from a global remote store.
        /// </summary>
        Global
    }
}
