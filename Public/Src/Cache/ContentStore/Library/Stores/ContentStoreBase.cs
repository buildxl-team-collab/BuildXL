// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BuildXL.Cache.ContentStore.Hashing;
using BuildXL.Cache.ContentStore.Interfaces.FileSystem;
using BuildXL.Cache.ContentStore.Interfaces.Results;
using BuildXL.Cache.ContentStore.Interfaces.Sessions;
using BuildXL.Cache.ContentStore.Interfaces.Tracing;
using BuildXL.Cache.ContentStore.Tracing;
using BuildXL.Cache.ContentStore.Sessions;
using BuildXL.Cache.ContentStore.Tracing.Internal;
using BuildXL.Cache.ContentStore.UtilitiesCore;
using BuildXL.Cache.ContentStore.Utils;
using BuildXL.Utilities.Tracing;
using BuildXL.Cache.ContentStore.Interfaces.Stores;

namespace BuildXL.Cache.ContentStore.Stores
{
    /// <summary>
    /// Base implementation of IContentSession. The purpose of having this class is to add common tracing
    /// behavior to all implementations.
    ///
    /// Note that this is intended to be subclassed by readonly content sessions but implements the full
    /// IContentSession, which is why methods for IContentSession are hidden by implementing them explicitly
    /// and making the Core implementations virtual and not abstract. The constraint that forced this design
    /// is that C# does not allow for multiple inheritance, and this was the only way to get base implementations
    /// for IContentSession.
    /// </summary>
    public abstract class ContentStoreBase<TSessionData> : StartupShutdownBase, IContentStore
    {
        protected CounterCollection<ContentSessionBaseCounters> Counters { get; } = new CounterCollection<ContentSessionBaseCounters>();

        /// <inheritdoc />
        public string Name { get; }

        /// <nodoc />
        protected virtual bool TracePinFinished => true;

        /// <nodoc />
        protected ContentStoreBase(string name)
        {
            Name = name;
        }

        /// <nodoc />
        protected virtual CounterSet GetCounters() => Counters.ToCounterSet();

        #region IContentStore Members

        protected abstract Task<DeleteResult> DeleteCoreAsync(OperationContext operationContext, ContentHash contentHash);

        public abstract void PostInitializationCompleted(Context context, BoolResult result);

        protected abstract Result<ContentSession> CreateSessionCore(Context context, string name, ImplicitPin implicitPin);

        public virtual CreateSessionResult<IReadOnlyContentSession> CreateReadOnlySession(Context context, string name, ImplicitPin implicitPin)
        {
            var result = CreateSessionCore(context, name, implicitPin);
            return result.Succeeded ? new CreateSessionResult<IReadOnlyContentSession>(result.Value) : new CreateSessionResult<IReadOnlyContentSession>(result);
        }

        public virtual CreateSessionResult<IContentSession> CreateSession(Context context, string name, ImplicitPin implicitPin)
        {
            var result = CreateSessionCore(context, name, implicitPin);
            return result.Succeeded ? new CreateSessionResult<IContentSession>(result.Value) : new CreateSessionResult<IContentSession>(result);
        }

        public virtual Task<GetStatsResult> GetStatsAsync(Context context)
        {
            return Task<GetStatsResult>.FromResult(new GetStatsResult(GetCounters()));
        }

        public Task<DeleteResult> DeleteAsync(Context context, ContentHash contentHash)
        {
            return WithOperationContext(
                context,
                CancellationToken.None,
                operationContext => operationContext.PerformNonResultOperationAsync(
                    Tracer,
                    () => DeleteCoreAsync(operationContext, contentHash),
                    traceOperationStarted: false,
                    traceOperationFinished: true,
                    counter: Counters[ContentSessionBaseCounters.Delete]));
        }

        #endregion IContentStore Members

        /// <nodoc />
        protected abstract Task<OpenStreamResult> OpenStreamCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            ContentHash contentHash,
            UrgencyHint urgencyHint,
            Counter retryCounter);

        /// <nodoc />
        protected abstract Task<PinResult> PinCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            ContentHash contentHash,
            UrgencyHint urgencyHint,
            Counter retryCounter);

        /// <nodoc />
        protected abstract Task<IEnumerable<Task<Indexed<PinResult>>>> PinCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            IReadOnlyList<ContentHash> contentHashes,
            UrgencyHint urgencyHint,
            Counter retryCounter,
            Counter fileCounter);

        /// <nodoc />
        protected abstract Task<PlaceFileResult> PlaceFileCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            ContentHash contentHash,
            AbsolutePath path,
            FileAccessMode accessMode,
            FileReplacementMode replacementMode,
            FileRealizationMode realizationMode,
            UrgencyHint urgencyHint,
            Counter retryCounter);

        /// <nodoc />
        protected abstract Task<IEnumerable<Task<Indexed<PlaceFileResult>>>> PlaceFileCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            IReadOnlyList<ContentHashWithPath> hashesWithPaths,
            FileAccessMode accessMode,
            FileReplacementMode replacementMode,
            FileRealizationMode realizationMode,
            UrgencyHint urgencyHint,
            Counter retryCounter);

        /// <nodoc />
        protected abstract Task<PutResult> PutFileCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            HashType hashType,
            AbsolutePath path,
            FileRealizationMode realizationMode,
            UrgencyHint urgencyHint,
            Counter retryCounter);

        /// <nodoc />
        protected abstract Task<PutResult> PutFileCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            ContentHash contentHash,
            AbsolutePath path,
            FileRealizationMode realizationMode,
            UrgencyHint urgencyHint,
            Counter retryCounter);

        // TODO: Should this do the logic to hash the stream and call PutStream with the hash
        // Or maybe there should be a common PutStreamTrustedCoreAsync which handles post hashing logic
        /// <nodoc />
        protected abstract Task<PutResult> PutStreamCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            HashType hashType,
            Stream stream,
            UrgencyHint urgencyHint,
            Counter retryCounter);

        /// <nodoc />
        protected abstract Task<PutResult> PutStreamCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            ContentHash contentHash,
            Stream stream,
            UrgencyHint urgencyHint,
            Counter retryCounter);

        protected class ContentSession : ContentSessionBase
        {
            protected ContentStoreBase<TSessionData> ContentStore { get; }
            protected TSessionData SessionData { get; }

            public ContentSession(ContentStoreBase<TSessionData> contentStore, string name) 
                : base(name)
            {
                ContentStore = contentStore;
            }

            protected override Tracer Tracer => throw new NotImplementedException();

            protected override Task<OpenStreamResult> OpenStreamCoreAsync(
                OperationContext operationContext,
                ContentHash contentHash,
                UrgencyHint urgencyHint,
                Counter retryCounter)
            {
                return ContentStore.OpenStreamCoreAsync(operationContext, SessionData, contentHash, urgencyHint, retryCounter);
            }

            protected override Task<PinResult> PinCoreAsync(
                OperationContext operationContext,
                ContentHash contentHash,
                UrgencyHint urgencyHint,
                Counter retryCounter)
            {
                return ContentStore.PinCoreAsync(operationContext, SessionData, contentHash, urgencyHint, retryCounter);
            }

            protected override Task<IEnumerable<Task<Indexed<PinResult>>>> PinCoreAsync(
                OperationContext operationContext,
                IReadOnlyList<ContentHash> contentHashes,
                UrgencyHint urgencyHint,
                Counter retryCounter,
                Counter fileCounter)
            {
                return ContentStore.PinCoreAsync(operationContext, SessionData, contentHashes, urgencyHint, retryCounter, fileCounter);
            }

            protected override Task<PutResult> PutFileCoreAsync(
                OperationContext operationContext,
                ContentHash contentHash,
                AbsolutePath path,
                FileRealizationMode realizationMode,
                UrgencyHint urgencyHint,
                Counter retryCounter)
            {
                return ContentStore.PutFileCoreAsync(operationContext, SessionData, contentHash, path, realizationMode, urgencyHint, retryCounter);
            }

            protected override Task<PutResult> PutFileCoreAsync(
                OperationContext operationContext,
                HashType hashType,
                AbsolutePath path,
                FileRealizationMode realizationMode,
                UrgencyHint urgencyHint,
                Counter retryCounter)
            {
                return ContentStore.PutFileCoreAsync(operationContext, SessionData, hashType, path, realizationMode, urgencyHint, retryCounter);
            }

            protected override Task<PutResult> PutStreamCoreAsync(
                OperationContext operationContext,
                ContentHash contentHash,
                Stream stream,
                UrgencyHint urgencyHint,
                Counter retryCounter)
            {
                return ContentStore.PutStreamCoreAsync(operationContext, SessionData, contentHash, stream, urgencyHint, retryCounter);
            }

            protected override Task<PutResult> PutStreamCoreAsync(
                OperationContext operationContext,
                HashType hashType,
                Stream stream,
                UrgencyHint urgencyHint,
                Counter retryCounter)
            {
                return ContentStore.PutStreamCoreAsync(operationContext, SessionData, hashType, stream, urgencyHint, retryCounter);
            }

            protected override Task<PlaceFileResult> PlaceFileCoreAsync(
                OperationContext operationContext,
                ContentHash contentHash,
                AbsolutePath path,
                FileAccessMode accessMode,
                FileReplacementMode replacementMode,
                FileRealizationMode realizationMode,
                UrgencyHint urgencyHint,
                Counter retryCounter)
            {
                return ContentStore.PlaceFileCoreAsync(operationContext, SessionData, contentHash, path, accessMode, replacementMode, realizationMode, urgencyHint, retryCounter);
            }

            protected override Task<IEnumerable<Task<Indexed<PlaceFileResult>>>> PlaceFileCoreAsync(
                OperationContext operationContext,
                IReadOnlyList<ContentHashWithPath> hashesWithPaths,
                FileAccessMode accessMode,
                FileReplacementMode replacementMode,
                FileRealizationMode realizationMode,
                UrgencyHint urgencyHint,
                Counter retryCounter)
            {
                return ContentStore.PlaceFileCoreAsync(operationContext, SessionData, hashesWithPaths, accessMode, replacementMode, realizationMode, urgencyHint, retryCounter);
            }
        }
    }
}
