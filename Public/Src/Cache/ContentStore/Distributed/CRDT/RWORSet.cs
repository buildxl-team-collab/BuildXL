// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Diagnostics.ContractsLight;

namespace BuildXL.Cache.ContentStore.Distributed.CRDT
{
    /// <summary>
    ///     Remove-Wins Observed-Remove Set Delta-based CRDT
    /// </summary>
    /// <typeparam name="I">
    ///     Type for the identity
    /// </typeparam>
    /// <typeparam name="V">
    ///     Type for the values stored within
    /// </typeparam>
    public class RWORSet<I, V>: ICausallyOrderable<I>
    {
        private class Token
        {
            public V Instance;

            /// <summary>
            /// When true, it means this is an observed-add token, when false, a observed-remove token.
            /// </summary>
            public bool Present;
        }

        private readonly DotKernel<I, Token> _kernel;
        private readonly I _identifier;

        /// <nodoc />
        public RWORSet(I identifier, DotContext<I> context = null)
        {
            Contract.Requires(identifier != null);

            _identifier = identifier;
            _kernel = new DotKernel<I, Token>(context ?? new DotContext<I>());
        }

        private RWORSet(I identifier, DotKernel<I, Token> kernel)
        {
            Contract.Requires(identifier != null);
            Contract.Requires(kernel != null);

            _identifier = identifier;
            _kernel = kernel;
        }

        /// <nodoc />
        public HashSet<V> Materialize()
        {
            var elements = new Dictionary<V, bool>();

            // TODO(jubayard): shouldn't this be doing things in temporal order? this doesn't guarantee that at all
            foreach (var differential in _kernel.Differential)
            {
                var value = differential.Value;
                if (elements.ContainsKey(value.Instance))
                {
                    elements[value.Instance] &= value.Present;
                }
                else
                {
                    elements.Add(value.Instance, value.Present);
                }
            }

            var result = new HashSet<V>();
            foreach (var element in elements)
            {
                if (element.Value)
                {
                    result.Add(element.Key);
                }
            }

            return result;
        }

        /// <nodoc />
        public bool Contains(V value)
        {
            // TODO(jubayard): this could be faster by re-using part of the materialize code and also always keeping a
            // materialized view.
            return Materialize().Contains(value);
        }

        /// <nodoc />
        public RWORSet<I, V> Add(V value)
        {
            return GenerateWithToken(value, observed: true);
        }

        /// <nodoc />
        public RWORSet<I, V> Remove(V value)
        {
            return GenerateWithToken(value, observed: false);
        }

        private RWORSet<I, V> GenerateWithToken(V value, bool observed)
        {
            var kernel = _kernel.Remove(new Token() { Instance = value, Present = true });
            kernel.Join(_kernel.Remove(new Token() { Instance = value, Present = false }));
            kernel.Join(_kernel.Add(_identifier, new Token { Instance = value, Present = observed }));
            return new RWORSet<I, V>(_identifier, kernel);
        }

        /// <nodoc />
        public RWORSet<I, V> Clear()
        {
            return new RWORSet<I, V>(_identifier, _kernel.Clear());
        }

        /// <nodoc />
        public void Join(RWORSet<I, V> other)
        {
            Contract.Requires(other != null);
            _kernel.Join(other._kernel);
        }

        public DotContext<I> Context() => _kernel.Context();
    }
}
