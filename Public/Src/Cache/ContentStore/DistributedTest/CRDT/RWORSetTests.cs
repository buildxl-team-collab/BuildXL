// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using BuildXL.Cache.ContentStore.Distributed.CRDT;
using FluentAssertions;
using Xunit;

namespace BuildXL.Cache.ContentStore.Distributed.Test.CRDT
{
    public class RWORSetTests
    {
        [Fact]
        public void ConcurrentRemoveAndAdd()
        {
            var sx = new RWORSet<char, string>('A');
            var sy = new RWORSet<char, string>('B');
            sx.Add("apple");
            sx.Remove("apple");

            sy.Add("juice");
            sy.Add("apple");

            sx.Join(sy);

            var result = sx.Materialize();
            result.Contains("apple").Should().BeFalse();
            result.Contains("juice").Should().BeTrue();
        }

        [Fact]
        public void ConcurrentRemoveAlwaysWins()
        {
            var x = new RWORSet<char, double>('A');
            var y = new RWORSet<char, double>('B');

            x.Add(3.14);
            x.Add(2.718);
            x.Remove(3.14);

            y.Add(3.14);

            x.Join(y);

            var result = x.Materialize();
            result.Contains(2.718).Should().BeTrue();
            result.Contains(3.14).Should().BeFalse();

            x.Clear();
            x.Join(y);

            // Clear removes everything, so the concurrent update turns out into empty.
            var result2 = x.Materialize();
            result2.Count.Should().Be(0);
        }
    }
}
