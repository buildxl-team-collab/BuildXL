// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Diagnostics.ContractsLight;

namespace BuildXL.Cache.ContentStore.Distributed.CRDT
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="I">
    ///     Type for the identity
    /// </typeparam>
    /// <typeparam name="V">
    ///     Type for the values stored within
    /// </typeparam>
    public class DotKernel<I, V>: ICausallyOrderable<I>
    {
        private readonly Dictionary<Dot<I>, V> _differential = new Dictionary<Dot<I>, V>();
        private readonly DotContext<I> _dotContext;

        /// <nodoc />
        public IReadOnlyDictionary<Dot<I>, V> Differential => _differential;

        /// <nodoc />
        public DotKernel(DotContext<I> context)
        {
            Contract.Requires(context != null);
            _dotContext = context;
        }

        /// <nodoc />
        public DotKernel<I, V> Add(I identity, V value)
        {
            Contract.Requires(identity != null);
            Contract.Requires(value != null);

            var dot = _dotContext.MakeDot(identity);
            _differential.Add(dot, value);

            // TODO(jubayard): clone the context?
            var delta = new DotKernel<I, V>(_dotContext);
            delta._differential.Add(dot, value);
            delta._dotContext.Insert(dot);
            return delta;
        }

        /// <nodoc />
        public DotKernel<I, V> Remove(V value)
        {
            Contract.Requires(value != null);

            // TODO(jubayard): clone the context?
            var delta = new DotKernel<I, V>(_dotContext);
            foreach (var differential in _differential)
            {
                if (differential.Value.Equals(value))
                {
                    // The removal of a value is an event in itself, and so the delta needs to acknowledge that.
                    delta._dotContext.Insert(differential.Key, forceCompaction: false);
                    _differential.Remove(differential.Key);
                }
            }
            delta._dotContext.Compact();
            return delta;
        }

        /// <nodoc />
        public DotKernel<I, V> Remove(Dot<I> dot)
        {
            Contract.Requires(dot != null);

            // TODO(jubayard): clone the context?
            var delta = new DotKernel<I, V>(_dotContext);
            if (_differential.TryGetValue(dot, out var differential))
            {
                delta._dotContext.Insert(dot, forceCompaction: false);
                _differential.Remove(dot);
            }
            delta._dotContext.Compact();
            return delta;
        }

        /// <nodoc />
        public DotKernel<I, V> Clear()
        {
            // TODO(jubayard): clone the context?
            var delta = new DotKernel<I, V>(_dotContext);
            foreach (var differential in _differential)
            {
                delta._dotContext.Insert(differential.Key, forceCompaction: false);
            }
            delta._dotContext.Compact();
            _differential.Clear();
            return delta;
        }

        /// <nodoc />
        public void Join(DotKernel<I, V> other)
        {
            Contract.Requires(other != null);

            if (this == other)
            {
                return;
            }

            // TODO(jubayard): make this more efficient by copying and traversing once over the smallest dictionary.
            foreach (var differential in _differential)
            {
                if (other._differential.ContainsKey(differential.Key))
                {
                    // Both contain the current dot, so we can safely skip
                    continue;
                }

                if (other._dotContext.Contains(differential.Key))
                {
                    // The other doesn't contain the current dot, but the context acknowledges it. This means that it
                    // has been removed in the future, so we need to remove it.
                    _differential.Remove(differential.Key);
                }
            }

            foreach (var differential in other._differential)
            {
                if (_differential.ContainsKey(differential.Key))
                {
                    // Both contain the current dot, so we can safely skip
                    continue;
                }

                if (!_dotContext.Contains(differential.Key))
                {
                    // The current one doesn't contain the current dot and does not acknowledge it, this means it is
                    // added in the future, and so we need to add it.
                    _differential.Add(differential.Key, differential.Value);
                }
            }

            // Join the contexts to finish computing the least upper bound.
            _dotContext.Join(other._dotContext);
        }

        public DotContext<I> Context() => _dotContext;
    }
}
