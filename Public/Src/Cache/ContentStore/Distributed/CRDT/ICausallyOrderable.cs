﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace BuildXL.Cache.ContentStore.Distributed.CRDT
{
    public interface ICausallyOrderable<I>
    {
        DotContext<I> Context();
    }
}
