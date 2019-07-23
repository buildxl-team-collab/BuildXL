// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.ContractsLight;
using System.Linq;

namespace BuildXL.Cache.ContentStore.Distributed.CRDT
{
    /// <nodoc />
    public class Dot<I>
    {
        /// <nodoc />
        public readonly I Identity;

        /// <nodoc />
        public readonly int Timestamp;

        /// <nodoc />
        public Dot(I identity, int timestamp)
        {
            Contract.Requires(identity != null);
            Contract.Requires(timestamp > 0);

            Identity = identity;
            Timestamp = timestamp;
        }
    }

    /// <summary>
    ///     A causal context that allows us to infer partial orders between events. This is used with a two purposes:
    ///      1. To track causality
    ///      2. To store causality deltas
    /// </summary>
    /// <typeparam name="I">
    ///     Type for the identity
    /// </typeparam>
    public class DotContext<I>
    {
        private readonly Dictionary<I, int> _versionVector = new Dictionary<I, int>();
        private readonly HashSet<Dot<I>> _dotCloud = new HashSet<Dot<I>>();

        /// <nodoc />
        public DotContext()
        {

        }

        private DotContext(DotContext<I> other)
        {
            foreach (var entry in other._versionVector)
            {
                _versionVector[entry.Key] = entry.Value;
            }

            foreach (var entry in other._dotCloud) {
                _dotCloud.Add(entry);
            }
        }

        /// <summary>
        ///     Check if the context has observed a given <see cref="Dot{K}"/>
        /// </summary>
        public bool Contains(Dot<I> dot)
        {
            Contract.Requires(dot != null);

            if (_versionVector.TryGetValue(dot.Identity, out var compactedTimestamp)) {
                if (dot.Timestamp <= compactedTimestamp)
                {
                    return true;
                }
            }

            if (_dotCloud.Count(t => t.Equals(dot)) > 0)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        ///     Rearranges the context into a more compact representation.
        /// </summary>
        public void Compact()
        {
            var runCompaction = false;

            do
            {
                runCompaction = false;

                var pendingRemoval = new List<Dot<I>>();
                foreach (var dot in _dotCloud)
                {
                    if (_versionVector.TryGetValue(dot.Identity, out var compactedTimestamp))
                    {
                        if (dot.Timestamp == compactedTimestamp + 1)
                        {
                            // The dot is precisely contiguous to the one we have stored, so we just increase the
                            // stored one. We need to run again, as we may have unblocked further compaction.
                            _versionVector[dot.Identity]++;
                            pendingRemoval.Add(dot);
                            runCompaction = true;
                        }
                        else if (dot.Timestamp <= compactedTimestamp)
                        {
                            // The dot in the cloud has already been dominated, so we can remove it. There can't be any
                            // new compaction opportunities in this case.
                            pendingRemoval.Add(dot);
                        }
                        else
                        {
                            // The new dot is strictly greater than the one in the causal context, and by more than 1,
                            // this means that we are missing at least one dot in between. We will need to wait.
                        }
                    }
                    else
                    {
                        if (dot.Timestamp == 1)
                        {
                            // The dot is not present in the compact context, which means its new. Thus, we just add it
                            // and allow for further compaction.
                            _versionVector.Add(dot.Identity, dot.Timestamp);
                            pendingRemoval.Add(dot);
                            runCompaction = true;
                        }
                        else
                        {
                            // As in the previous else case, we are missing the initial dot here, so we can't do
                            // anything but wait.
                        }
                    }
                }

                foreach (var dot in pendingRemoval)
                {
                    _dotCloud.Remove(dot);
                }
            } while (runCompaction);
        }

        /// <summary>
        ///     Generates a new dot for a given identity.
        /// </summary>
        public Dot<I> MakeDot(I identity)
        {
            Contract.Requires(identity != null);

            if (_versionVector.ContainsKey(identity))
            {
                _versionVector[identity]++;
                return new Dot<I>(identity, _versionVector[identity]);
            }

            _versionVector[identity] = 1;
            return new Dot<I>(identity, 1);
        }

        /// <summary>
        ///     Insert a given <see cref="Dot{K}"/>.
        /// </summary>
        /// <param name="forceCompaction">
        ///     When this parameter is false, the dot is inserted in a temporary buffer for updates that have yet to
        ///     be compacted.
        /// </param>
        public void Insert(Dot<I> dot, bool forceCompaction=true)
        {
            Contract.Requires(dot != null);

            _dotCloud.Add(dot);

            if (forceCompaction)
            {
                Compact();
            }
        }

        /// <summary>
        ///     Idempotent join that generates a least upper bound in the joint semilattice of
        ///     <see cref="DotContext{K}"/>
        /// </summary>
        public void Join(DotContext<I> other)
        {
            Contract.Requires(other != null);

            // Avoid wasting cycles
            if (this == other)
            {
                return;
            }

            // TODO(jubayard): make this more efficient by copying and traversing once over the smallest dictionary.
            foreach (var dot in _versionVector.ToList())
            {
                if (!other._versionVector.TryGetValue(dot.Key, out var otherCompactedTimestamp))
                {
                    continue;
                }

                _versionVector[dot.Key] = Math.Max(dot.Value, otherCompactedTimestamp);
            }

            foreach (var dot in other._versionVector)
            {
                if (_versionVector.TryGetValue(dot.Key, out var myCompactedTimestamp))
                {
                    _versionVector[dot.Key] = Math.Max(dot.Value, myCompactedTimestamp);
                } else
                {
                    _versionVector[dot.Key] = dot.Value;
                }
            }

            // Add everything from the other cloud and force compaction at the end
            foreach (var dot in other._dotCloud)
            {
                Insert(dot, forceCompaction: false);
            }

            Compact();
        }

        public DotContext<I> DeepCopy()
        {
            return new DotContext<I>(this);
        }
    }
}
