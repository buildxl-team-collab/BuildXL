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
    ///     Note that this is intended to be subclassed by readonly content sessions but implements the full
    /// IContentSession, which is why methods for IContentSession are hidden by implementing them explicitly
    /// and making the Core implementations virtual and not abstract. The constraint that forced this design
    /// is that C# does not allow for multiple inheritance, and this was the only way to get base implementations
    /// for IContentSession.
    /// </summary>
    public abstract class ReadOnlyContentStoreBase<TSessionData> : StartupShutdownBase, IContentStore
    {
        protected CounterCollection<ContentSessionBaseCounters> Counters { get; } = new CounterCollection<ContentSessionBaseCounters>();

        /// <inheritdoc />
        public string Name { get; }

        /// <nodoc />
        protected virtual bool TracePinFinished => true;

        /// <nodoc />
        protected ReadOnlyContentStoreBase(string name)
        {
            Name = name;
        }

        /// <inheritdoc />
        public Task<OpenStreamResult> OpenStreamAsync(
            Context context,
            TSessionData sessionData,
            ContentHash contentHash,
            CancellationToken token,
            UrgencyHint urgencyHint = UrgencyHint.Nominal)
        {
            return WithOperationContext(
                context,
                token,
                operationContext => operationContext.PerformOperationAsync(
                    Tracer,
                    () => OpenStreamCoreAsync(operationContext, sessionData,contentHash, urgencyHint, Counters[ContentSessionBaseCounters.OpenStreamRetries]),
                    counter: Counters[ContentSessionBaseCounters.OpenStream]));
        }

        /// <nodoc />
        protected abstract Task<OpenStreamResult> OpenStreamCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            ContentHash contentHash,
            UrgencyHint urgencyHint,
            Counter retryCounter);

        /// <inheritdoc />
        public Task<PinResult> PinAsync(
            Context context,
            TSessionData sessionData,
            ContentHash contentHash,
            CancellationToken token,
            UrgencyHint urgencyHint = UrgencyHint.Nominal)
        {
            return WithOperationContext(
                context,
                token,
                operationContext => operationContext.PerformOperationAsync(
                    Tracer,
                    () => PinCoreAsync(operationContext, sessionData,contentHash, urgencyHint, Counters[ContentSessionBaseCounters.PinRetries]),
                    traceOperationStarted: false,
                    traceOperationFinished: TracePinFinished,
                    extraEndMessage: _ => $"input=[{contentHash.ToShortString()}]",
                    counter: Counters[ContentSessionBaseCounters.Pin]));
        }

        /// <nodoc />
        protected abstract Task<PinResult> PinCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            ContentHash contentHash,
            UrgencyHint urgencyHint,
            Counter retryCounter);

        /// <inheritdoc />
        public Task<IEnumerable<Task<Indexed<PinResult>>>> PinAsync(
            Context context,
            TSessionData sessionData,
            IReadOnlyList<ContentHash> contentHashes,
            CancellationToken token,
            UrgencyHint urgencyHint = UrgencyHint.Nominal)
        {
            return WithOperationContext(
                context,
                token,
                operationContext => operationContext.PerformNonResultOperationAsync(
                    Tracer,
                    () => PinCoreAsync(operationContext, sessionData,contentHashes, urgencyHint, Counters[ContentSessionBaseCounters.PinBulkRetries], Counters[ContentSessionBaseCounters.PinBulkFileCount]),
                    extraEndMessage: results =>
                    {
                        var resultString = string.Join(",", results.Select(task =>
                        {
                            // Since all bulk operations are constructed with Task.FromResult, it is safe to just access the result;
                            var result = task.Result;
                            return $"{contentHashes[result.Index].ToShortString()}:{result.Item}";
                        }));

                        return $"Hashes=[{resultString}]";
                    },
                    traceOperationStarted: false,
                    counter: Counters[ContentSessionBaseCounters.PinBulk]));
        }

        /// <nodoc />
        protected abstract Task<IEnumerable<Task<Indexed<PinResult>>>> PinCoreAsync(
            OperationContext operationContext,
            TSessionData sessionData,
            IReadOnlyList<ContentHash> contentHashes,
            UrgencyHint urgencyHint,
            Counter retryCounter,
            Counter fileCounter);

        /// <inheritdoc />
        public Task<PlaceFileResult> PlaceFileAsync(
            Context context,
            TSessionData sessionData,
            ContentHash contentHash,
            AbsolutePath path,
            FileAccessMode accessMode,
            FileReplacementMode replacementMode,
            FileRealizationMode realizationMode,
            CancellationToken token,
            UrgencyHint urgencyHint = UrgencyHint.Nominal)
        {
            return WithOperationContext(
                context,
                token,
                operationContext => operationContext.PerformOperationAsync(
                    Tracer,
                    () => PlaceFileCoreAsync(operationContext, sessionData,contentHash, path, accessMode, replacementMode, realizationMode, urgencyHint, Counters[ContentSessionBaseCounters.PlaceFileRetries]),
                    extraStartMessage: $"({contentHash.ToShortString()},{path},{accessMode},{replacementMode},{realizationMode})",
                    extraEndMessage: (_) => $"input={contentHash.ToShortString()}",
                    counter: Counters[ContentSessionBaseCounters.PlaceFile]));
        }

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

        /// <inheritdoc />
        public Task<IEnumerable<Task<Indexed<PlaceFileResult>>>> PlaceFileAsync(
            Context context,
            TSessionData sessionData,
            IReadOnlyList<ContentHashWithPath> hashesWithPaths,
            FileAccessMode accessMode,
            FileReplacementMode replacementMode,
            FileRealizationMode realizationMode,
            CancellationToken token,
            UrgencyHint urgencyHint = UrgencyHint.Nominal)
        {
            return WithOperationContext(
                context,
                token,
                operationContext => operationContext.PerformNonResultOperationAsync(
                    Tracer,
                    () => PlaceFileCoreAsync(operationContext, sessionData,hashesWithPaths, accessMode, replacementMode, realizationMode, urgencyHint, Counters[ContentSessionBaseCounters.PlaceFileBulkRetries]),
                    traceOperationStarted: false,
                    traceOperationFinished: false,
                    counter: Counters[ContentSessionBaseCounters.PlaceFileBulk]));
        }

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
        protected virtual CounterSet GetCounters() => Counters.ToCounterSet();

        protected abstract Task<DeleteResult> DeleteCoreAsync(OperationContext operationContext, ContentHash contentHash);

        public virtual void PostInitializationCompleted(Context context, BoolResult result)
        {
        }

        public CreateSessionResult<IReadOnlyContentSession> CreateReadOnlySession(Context context, string name, ImplicitPin implicitPin)
        {
            throw new NotImplementedException();
        }

        public CreateSessionResult<IContentSession> CreateSession(Context context, string name, ImplicitPin implicitPin)
        {
            throw new NotImplementedException();
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
                    counter: Counters[ContentSessionBaseCounters.PlaceFileBulk]));
        }
    }
}
