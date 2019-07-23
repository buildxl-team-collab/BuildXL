// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using BuildXL.Cache.ContentStore.Distributed.CRDT;
using FluentAssertions;
using Xunit;

namespace BuildXL.Cache.ContentStore.Distributed.Test.CRDT
{
    public class ORMapTests
    {
        [Fact]
        public void ConcurrentAddAndRemoveKey()
        {
            var operations = new ORMapRequiredOperations<char, RWORSet<char, string>>() {
                Default = (identity, context) => new RWORSet<char, string>(identity, context),
                Join = (v1, v2) => v1.Join(v2),
                ObtainClearContext = (v) => v.Clear().Context(),
            };

            var mx = new ORMap<char, string, RWORSet<char, string>>(operations, 'X');
            var my = new ORMap<char, string, RWORSet<char, string>>(operations, 'Y');

            mx["paint"].Add("blue");
            mx["sound"].Add("loud");
            mx["sound"].Add("soft");

            my["paint"].Add("red");
            my["number"].Add("42");

            mx.Join(my);

            var materialized = mx.Materialize();
            materialized.Keys.Should().Contain(new [] { "paint", "sound", "number" });

            my["number"].Remove("42");
            mx.Join(my);

            var materialized2 = mx.Materialize();
            materialized2.Keys.Should().Contain(new[] { "paint", "sound" });
        }
    }
}
