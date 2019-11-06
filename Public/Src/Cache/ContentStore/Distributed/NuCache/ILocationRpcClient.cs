// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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
    /// Interface that represents a client for a <see cref="ILocationStore"/> exposed as a service
    /// </summary>
    public interface ILocationRpcClient : IStartupShutdownSlim
    {
        /// <summary>
        /// Sends an rpc request
        /// </summary>
        Task<Result<byte[]>> Send(OperationContext context, RequestType type, byte[] requestPayload);

        /// <nodoc />
        CounterSet GetCounters(OperationContext context);
    }

    public enum RequestType
    {
        GetBulk,

        /// <summary>
        /// WIP: Can send appreciably less data for this request, so its been separated out
        /// </summary>
        RegisterLocalLocation,
        RegisterLocations,
        PutBlob,
        GetBlob
    }

    public interface ILocationRpcServer : IStartupShutdownSlim
    {
        /// <summary>
        /// Sends an rpc request
        /// </summary>
        Task<Result<byte[]>> Receive(OperationContext context, RequestType type, string sender, byte[] requestPayload);
    }
}
