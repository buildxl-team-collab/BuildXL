// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.ContractsLight;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BuildXL.Cache.ContentStore.Distributed.CRDT
{
    /// <summary>
    ///     Ugly hack to get around the fact that C# does not have SFINAE
    /// </summary>
    public class ORMapRequiredOperations<I, V>
    {
        public Func<I, DotContext<I>, V> Default;
        public Action<V, V> Join;
        public Func<V, DotContext<I>> ObtainClearContext;
    }

    /// <summary>
    ///     Observed-Remove Map CRDT.
    /// </summary>
    /// <typeparam name="I">
    ///     Type for the identity
    /// </typeparam>
    /// <typeparam name="K">
    ///     Type for the keys
    /// </typeparam>
    /// <typeparam name="V">
    ///     Type for the values stored within. Must be one of the following CRDTs:
    ///      - AWORSet
    ///      - RWORSet
    ///      - MVRegister
    ///      - EWFlag
    ///      - DWFlag
    ///      - CCounter
    ///      - ORMap
    /// </typeparam>
    public class ORMap<I, K, V>: ICausallyOrderable<I>
        where V: class
    {
        private readonly Dictionary<K, V> _entries = new Dictionary<K, V>();
        private DotContext<I> _dotContext;
        private readonly I _identity;
        private readonly ORMapRequiredOperations<I, V> _operations;

        /// <nodoc />
        public ORMap(ORMapRequiredOperations<I, V> operations, I identity, DotContext<I> dotContext = null)
        {
            Contract.Requires(operations != null);
            Contract.Requires(identity != null);

            _operations = operations;
            _identity = identity;
            _dotContext = dotContext ?? new DotContext<I>();
        }

        public V this[K key]
        {
            get
            {
                if (!_entries.ContainsKey(key))
                {
                    _entries.Add(key, _operations.Default(_identity, _dotContext));
                }

                return _entries[key];
            }
            set
            {
                // This is never supported because it throws off the context maths.
                throw new NotSupportedException();
            }
        }

        public ORMap<I, K, V> Remove(K key)
        {
            if (_entries.ContainsKey(key))
            {
                var delta = new ORMap<I, K, V>(_operations, _identity, _operations.ObtainClearContext(_entries[key]));
                _entries.Remove(key);
                return delta;
            }
            else
            {
                return new ORMap<I, K, V>(_operations, _identity);
            }
        }

        public ORMap<I, K, V> Clear()
        {
            if (_entries.Count == 0)
            {
                return new ORMap<I, K, V>(_operations, _identity);
            }

            var delta = new ORMap<I, K, V>(_operations, _identity);
            var context = delta._dotContext;
            foreach (var entry in _entries)
            {
                context.Join(_operations.ObtainClearContext(entry.Value));
            }

            _entries.Clear();
            return delta;
        }

        public void Join(ORMap<I, K, V> other)
        {
            // The context is shared among all live instances and stored values, so we need to clone here. This is a
            // very expensive
            var initialContext = _dotContext.DeepCopy();

            foreach (var entry in _entries)
            {
                if (other._entries.TryGetValue(entry.Key, out var otherValue))
                {
                    _operations.Join(entry.Value, otherValue);
                    _dotContext = initialContext.DeepCopy();
                }
                else
                {
                    _operations.Join(entry.Value, _operations.Default(_identity, other._dotContext));
                    _dotContext = initialContext.DeepCopy();
                }
            }

            foreach (var entry in other._entries)
            {
                if (_entries.ContainsKey(entry.Key))
                {
                    continue;
                }

                _operations.Join(this[entry.Key], entry.Value);
                _dotContext = initialContext;
            }

            _dotContext.Join(other._dotContext);
        }

        public DotContext<I> Context() => _dotContext;

        public IReadOnlyDictionary<K, V> Materialize() => _entries;
    }
}
