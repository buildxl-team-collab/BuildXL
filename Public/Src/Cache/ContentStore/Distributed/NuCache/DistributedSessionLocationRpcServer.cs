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
using BuildXL.Utilities;
using BuildXL.Utilities.Collections;
using BuildXL.Utilities.Tracing;

namespace BuildXL.Cache.ContentStore.Distributed.NuCache
{
    /// <summary>
    /// Interface that represents a location store for tracking distributed content
    /// </summary>
    public class DistributedSessionLocationRpcServer : StartupShutdownSlimBase, ILocationRpcServer
    {
        private DistributedSessionLocationStore _store;
        private DistributedSessionLocationRpcSerializer _serializer;

        protected override Tracer Tracer => throw new NotImplementedException();

        public async Task<Result<byte[]>> Receive(OperationContext context, RequestType type, string sender, byte[] requestPayload)
        {
            // WIP: Logging?

            switch (type)
            {
                case RequestType.GetBulk:
                    {
                        var args = _serializer.DeserializeGetBulkArguments(requestPayload);
                        var result = await _store.GetBulkAsync(context, args.hashes, args.origin).ThrowIfFailureAsync();
                        return _serializer.SerializeGetBulkResult(result);
                    }
                case RequestType.RegisterLocalLocation:
                    {
                        var args = _serializer.DeserializeRegisterLocalLocationArguments(requestPayload);
                        var result = await _store.RegisterLocationAsync(context, args.location, args.hashes).ThrowIfFailure();
                        return _serializer.SerializeBoolResult(result);
                    }
                case RequestType.RegisterLocations:
                    {
                        var args = _serializer.DeserializeRegisterEntriesArguments(requestPayload);
                        var result = await _store.RegisterEntries(context, args).ThrowIfFailure();
                        return _serializer.SerializeBoolResult(result);
                    }
                case RequestType.PutBlob:
                    {
                        var args = _serializer.DeserializePutBlobArguments(requestPayload);
                        var result = await _store.PutBlobAsync(context, args.hash, args.blob).ThrowIfFailure();
                        return _serializer.SerializeBoolResult(result);
                    }
                case RequestType.GetBlob:
                    {
                        var args = _serializer.DeserializeGetBlobArguments(requestPayload);
                        return await _store.GetBlobAsync(context, args).ThrowIfFailure();
                    }
                default:
                    break;
            }

            throw new NotImplementedException();
        }
    }

    public class DistributedSessionLocationRpcSerializer
    {
        private readonly ObjectPool<StreamBinaryWriter> _writerPool = new ObjectPool<StreamBinaryWriter>(() => new StreamBinaryWriter(), w => w.ResetPosition());
        private readonly ObjectPool<StreamBinaryReader> _readerPool = new ObjectPool<StreamBinaryReader>(() => new StreamBinaryReader(), r => { });

        public (IReadOnlyList<ContentHash> hashes, GetBulkOrigin origin) DeserializeGetBulkArguments(byte[] payload)
        {
            using var readerWrapper = _readerPool.GetInstance();
            var reader = readerWrapper.Instance;
            return reader.Deserialize(new ArraySegment<byte>(payload), r => 
                (r.ReadReadOnlyList(r => new ContentHash(r)), (GetBulkOrigin)r.ReadByte()));
        }

        public (MachineId location, IReadOnlyList<ContentHashWithSize> hashes) DeserializeRegisterLocalLocationArguments(byte[] payload)
        {
            using var readerWrapper = _readerPool.GetInstance();
            var reader = readerWrapper.Instance;
            return reader.Deserialize(new ArraySegment<byte>(payload), r => 
                (MachineId.Deserialize(r), r.ReadReadOnlyList(r => new ContentHashWithSize(new ContentHash(r), r.ReadInt64Compact()))));
        }

        public IReadOnlyList<(ShortHash hash, ContentLocationEntry entry)> DeserializeRegisterEntriesArguments(byte[] payload)
        {
            using var readerWrapper = _readerPool.GetInstance();
            var reader = readerWrapper.Instance;
            //return reader.Deserialize(new ArraySegment<byte>(payload), r =>
            //    (ShortHash, r.ReadReadOnlyList(r => new ContentHashWithSize(new ContentHash(r), r.ReadInt64Compact()))));
            throw new NotImplementedException();
        }

        public (ContentHash hash, ArraySegment<byte> blob) DeserializePutBlobArguments(byte[] payload)
        {
            throw new NotImplementedException();
        }

        public ContentHash DeserializeGetBlobArguments(byte[] payload)
        {
            throw new NotImplementedException();
        }

        public byte[] SerializeGetBulkResult(IReadOnlyList<ContentLocationEntry> result)
        {
            throw new NotImplementedException();
        }

        public byte[] SerializeBoolResult(BoolResult result)
        {
            throw new NotImplementedException();
        }
    }
}
