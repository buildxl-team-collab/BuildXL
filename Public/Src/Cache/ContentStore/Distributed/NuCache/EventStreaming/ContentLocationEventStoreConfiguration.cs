// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics.ContractsLight;

namespace BuildXL.Cache.ContentStore.Distributed.NuCache.EventStreaming
{
    /// <summary>
    /// Configuration type for <see cref="ContentLocationEventStore"/> family of types.
    /// </summary>
    public abstract class ContentLocationEventStoreConfiguration
    {
        /// <summary>
        /// The number of events which forces an event batch to be sent
        /// </summary>
        public int EventBatchSize { get; set; } = 200;

        /// <summary>
        /// The number of events which forces an event batch to be sent
        /// </summary>
        public TimeSpan EventNagleInterval { get; set; } = TimeSpan.FromMinutes(2);

        /// <summary>
        /// An epoch used for reseting event processing.
        /// </summary>
        public string Epoch { get; set; }

        /// <summary>
        /// Specifies the delay of the first event processed after the epoch is reset.
        /// </summary>
        public TimeSpan NewEpochEventStartCursorDelay { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// If enabled, serialized entries would be deserialized back to make sure the serialization/deserialization process is correct.
        /// </summary>
        public bool SelfCheckSerialization { get; set; } = false;
    }

    /// <summary>
    /// Configuration type for <see cref="MemoryContentLocationEventStore"/>.
    /// </summary>
    public sealed class MemoryContentLocationEventStoreConfiguration : ContentLocationEventStoreConfiguration
    {
        /// <nodoc />
        public MemoryContentLocationEventStoreConfiguration()
        {
            EventBatchSize = 1;
        }

        /// <summary>
        /// Global in-memory event hub used for testing purposes.
        /// </summary>
        public MemoryContentLocationEventStore.EventHub Hub { get; } = new MemoryContentLocationEventStore.EventHub();
    }

    /// <summary>
    /// Configuration type for event hub-based content location event store.
    /// </summary>
    public sealed class EventHubContentLocationEventStoreConfiguration : ContentLocationEventStoreConfiguration
    {
        /// <inheritdoc />
        public EventHubContentLocationEventStoreConfiguration(
            string eventHubName,
            string eventHubConnectionString,
            string consumerGroupName,
            string epoch)
        {
            Contract.Requires(!string.IsNullOrEmpty(eventHubName));
            Contract.Requires(!string.IsNullOrEmpty(eventHubConnectionString));
            Contract.Requires(!string.IsNullOrEmpty(consumerGroupName));

            EventHubName = eventHubName;
            EventHubConnectionString = eventHubConnectionString;
            ConsumerGroupName = consumerGroupName;
            Epoch = epoch ?? string.Empty;
        }

        /// <summary>
        /// Event Hub name (a.k.a. Event Hub's entity path).
        /// </summary>
        public string EventHubName { get; }

        /// <nodoc />
        public string EventHubConnectionString { get; }

        /// <nodoc />
        public string ConsumerGroupName { get; set; }

        /// <summary>
        /// The max concurrency to use for events processing.
        /// </summary>
        public int MaxEventProcessingConcurrency { get; set; } = 16;

        /// <summary>
        /// The size of the queue used for concurrent event processing.
        /// </summary>
        public int EventProcessingMaxQueueSize { get; set; } = 100;

        /// <summary>
        /// The maximum sequence point to process. After it has been handled, we stop all processing of events and
        /// return. Used only for performance benchmarking.
        /// </summary>
        public long? MaximumSequenceNumberToProcess { get; set; } = null;

        /// <summary>
        /// Creates another configuration instance with a given <paramref name="consumerGroupName"/>.
        /// </summary>
        public EventHubContentLocationEventStoreConfiguration WithConsumerGroupName(string consumerGroupName)
            => new EventHubContentLocationEventStoreConfiguration(EventHubName, EventHubConnectionString, consumerGroupName, Epoch);

    }
}
