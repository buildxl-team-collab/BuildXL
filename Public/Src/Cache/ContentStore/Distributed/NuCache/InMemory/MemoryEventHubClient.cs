// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using BuildXL.Cache.ContentStore.Distributed.NuCache.EventStreaming;
using BuildXL.Cache.ContentStore.Interfaces.Results;
using BuildXL.Cache.ContentStore.Tracing;
using BuildXL.Cache.ContentStore.Tracing.Internal;
using BuildXL.Cache.ContentStore.Utils;
using BuildXL.Utilities.Threading;
using BuildXL.Utilities.Tracing;
using Microsoft.Azure.EventHubs;

namespace BuildXL.Cache.ContentStore.Distributed.NuCache
{
    /// <summary>
    /// An event hub client which interacts with a test in-process event hub service
    /// </summary>
    public sealed class MemoryEventHubClient : StartupShutdownSlimBase, IEventHubClient
    {
        private readonly EventHub _hub;
        private bool _processing = false;
        private readonly ReadWriteLock _lock = ReadWriteLock.Create();
        private OperationContext _context;
        private IPartitionReceiveHandler _receiver;

        private readonly BlockingCollection<EventData> _queue = new BlockingCollection<EventData>();

        protected override Tracer Tracer { get; } = new Tracer(nameof(MemoryEventHubClient));

        /// <inheritdoc />
        public MemoryEventHubClient(MemoryContentLocationEventStoreConfiguration configuration)
        {
            _hub = configuration.Hub;
            _hub.OnEvent += HubOnEvent;
        }

        /// <inheritdoc />
        protected override Task<BoolResult> StartupCoreAsync(OperationContext context)
        {
            Tracer.Info(context, "Initializing in-memory content location event store.");

            _context = context;
            return base.StartupCoreAsync(context);
        }

        /// <inheritdoc />
        protected override Task<BoolResult> ShutdownCoreAsync(OperationContext context)
        {
            _hub.OnEvent -= HubOnEvent;
            _queue.CompleteAdding();

            return base.ShutdownCoreAsync(context);
        }

        private void HubOnEvent(EventData eventData)
        {
            using (_lock.AcquireReadLock())
            {
                if (_processing)
                {
                    DispatchAsync(_context, eventData).GetAwaiter().GetResult();
                }
                else
                {
                    // Processing in suspended enqueue
                    _queue.Add(eventData);
                }
            }
        }

        /// <inheritdoc />
        public BoolResult StartProcessing(OperationContext context, EventSequencePoint sequencePoint, IPartitionReceiveHandler processor)
        {
            using (_lock.AcquireWriteLock())
            {
                _processing = true;

                while (_queue.TryTake(out var eventData))
                {
                    DispatchAsync(context, eventData).GetAwaiter().GetResult();
                }
            }

            return BoolResult.Success;
        }

        /// <inheritdoc />
        public BoolResult SuspendProcessing(OperationContext context)
        {
            using (_lock.AcquireWriteLock())
            {
                _processing = false;
            }

            return BoolResult.Success;
        }

        /// <inheritdoc />
        public Task SendAsync(OperationContext context, EventData eventData)
        {
            _hub.Send(eventData);
            return BoolResult.SuccessTask;
        }

        public Task DispatchAsync(OperationContext context, EventData eventData)
        {
            return _receiver.ProcessEventsAsync(new[] { eventData });
        }

        /// <summary>
        /// In-memory event hub for communicating between different event store instances in memory.
        /// </summary>
        public sealed class EventHub
        {
            // EventData system property names (copied from event hub codebase)
            public const string EnqueuedTimeUtcName = "x-opt-enqueued-time";
            public const string SequenceNumberName = "x-opt-sequence-number";

            private readonly PropertyInfo _systemPropertiesPropertyInfo = typeof(EventData).GetProperty(nameof(EventData.SystemProperties));

            private long _sequenceNumber;

            private readonly object _syncLock = new object();

            /// <nodoc />
            public event Action<EventData> OnEvent;

            /// <nodoc />
            public void Send(EventData eventData)
            {
                lock (_syncLock)
                {
                    LockFreeSend(eventData);
                }
            }

            /// <summary>
            /// <see cref="Send(ContentLocationEventData)"/>, without the lock. Used because of perf benchmarks
            /// getting contention on <see cref="_syncLock"/>. Since it is not part of the usual implementation of
            /// EventHub and not clear it is required for correctness, it is removed here.
            /// </summary>
            public void LockFreeSend(EventData eventData)
            {
                // HACK: Use reflect to set system properties property since its internal
                _systemPropertiesPropertyInfo.SetValue(eventData, Activator.CreateInstance(typeof(EventData.SystemPropertiesCollection)));

                var sequenceNumber = Interlocked.Increment(ref _sequenceNumber);
                eventData.SystemProperties[SequenceNumberName] = sequenceNumber;
                eventData.SystemProperties[EnqueuedTimeUtcName] = DateTime.UtcNow;

                OnEvent(eventData);
            }
        }
    }
}
