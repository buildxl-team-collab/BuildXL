﻿using System;
using System.Collections.Generic;
using System.Diagnostics.ContractsLight;
using BuildXL.Cache.ContentStore.Interfaces.Logging;

namespace BuildXL.Cache.Monitor.App
{
    internal static class Utilities
    {
        public static IEnumerable<T> Yield<T>(this T item)
        {
            yield return item;
        }

        public static void SplitBy<T>(this IEnumerable<T> enumerable, Func<T, bool> predicate, ICollection<T> trueSet, ICollection<T> falseSet)
        {
            // TODO(jubayard): this function can be split in two cases, find the first index at which the predicate is
            // true, and find all entries for which the predicate is true. Need to evaluate case-by-case.
            Contract.RequiresNotNull(enumerable);
            Contract.RequiresNotNull(predicate);
            Contract.RequiresNotNull(trueSet);
            Contract.RequiresNotNull(falseSet);

            foreach (var entry in enumerable)
            {
                if (predicate(entry))
                {
                    trueSet.Add(entry);
                }
                else
                {
                    falseSet.Add(entry);
                }
            }
        }

        public static void SeverityFromThreshold<T>(T value, T threshold, T errorThreshold, Action<Severity, T> action, IComparer<T> comparer = null, Severity severity = Severity.Warning)
        {
            if (comparer == null)
            {
                comparer = Comparer<T>.Default;
            }

            if (comparer.Compare(value, threshold) >= 0)
            {
                if (comparer.Compare(value, errorThreshold) >= 0)
                {
                    action(severity + 1, errorThreshold);
                }
                else
                {
                    action(severity, threshold);
                }
            }
        }
    }
}
