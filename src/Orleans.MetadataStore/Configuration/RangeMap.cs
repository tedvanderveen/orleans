using System;
using System.Diagnostics.Contracts;

namespace Orleans.MetadataStore
{
    public struct RangeMap
    {
        public RangeMap(int[] upperBounds, int[][] rangeNodes)
        {
            UpperBounds = upperBounds;
            RangeNodes = rangeNodes;
        }

        /// <summary>
        /// Upper bounds of successive ranges.
        /// </summary>
        public int[] UpperBounds { get; }

        /// <summary>
        /// Indices (into <see cref="ReplicaSetConfiguration.Nodes"/>) corresponding to each range in <see cref="UpperBounds"/>.
        /// </summary>
        public int[][] RangeNodes { get; }

        [Pure]
        public static RangeMap SplitAt(RangeMap range, int newBoundary)
        {
            var splitIndex = range.GetRangeIndexForKey(newBoundary);
            if (newBoundary == range.UpperBounds[splitIndex]) throw new ArgumentException($"Cannot split a range at an existing bound. Bound = {newBoundary}", nameof(newBoundary));

            var oldLength = range.UpperBounds.Length;
            var newBounds = new int[oldLength + 1];
            var newNodes = new int[oldLength + 1][];

            var added = 0;
            for (var i = 0; i < oldLength; i++)
            {
                if (i == splitIndex)
                {
                    // Duplicate the nodes at the boundary.
                    newBounds[i] = newBoundary;
                    newNodes[i] = range.RangeNodes[i];

                    newBounds[i + 1] = range.UpperBounds[i];
                    newNodes[i + 1] = range.RangeNodes[i];
                    added = 1;
                }
                else
                {
                    newBounds[i + added] = range.UpperBounds[i];
                    newNodes[i + added] = range.RangeNodes[i];
                }
            }

            return new RangeMap(newBounds, newNodes);
        }

        [Pure]
        public static RangeMap JoinAt(RangeMap range, int boundary)
        {
            var oldLength = range.UpperBounds.Length;
            var newBounds = new int[oldLength - 1];
            var newNodes = new int[oldLength - 1][];
            var removed = 0;
            for (var i = 0; i < range.UpperBounds.Length; i++)
            {
                if (range.UpperBounds[i] == boundary)
                {
                    if (i + 1 == range.UpperBounds.Length) throw new InvalidOperationException("Cannot join at the upper-most bound");

                    var current = range.RangeNodes[i];
                    var next = range.RangeNodes[i + 1];

                    if (current.Length != next.Length)
                        throw new InvalidOperationException("Ranges with non-identical nodes cannot be joined.");

                    for (var j = 0; j < current.Length; j++)
                        if (current[j] != next[j])
                            throw new InvalidOperationException("Ranges with non-identical nodes cannot be joined.");

                    removed = 1;
                    continue;
                }

                if (i - removed == newBounds.Length) throw new InvalidOperationException($"Cannot remove boundary {boundary} from a set which does not contain it.");

                newBounds[i - removed] = range.UpperBounds[i];
                newNodes[i - removed] = range.RangeNodes[i];
            }

            return new RangeMap(newBounds, newNodes);
        }
        
        [Pure]
        public static RangeMap AddNode(RangeMap range, int key, int toAdd)
        {
            var index = range.GetRangeIndexForKey(key);
            var updated = new int[range.RangeNodes.Length][];
            for (var i = 0; i < range.RangeNodes.Length; i++)
            {
                int[] current;
                if (i == index)
                {
                    // Copy the old nodes to a new array and add the new node at the end.
                    var existing = range.RangeNodes[i];
                    current = new int[existing.Length + 1];
                    var added = 0;
                    for (var j = 0; added == 0 || j < existing.Length; j++)
                    {
                        // If adding the new node here would maintain sorting order, add it.
                        var isLastElement = j == existing.Length;
                        if (added == 0 && (isLastElement || toAdd < existing[j]))
                        {
                            current[j] = toAdd;
                            added = 1;

                            if (isLastElement) break;
                        }

                        if (existing[j] == toAdd) throw new InvalidOperationException($"Cannot add node {toAdd} to a set which already contains it");
                        current[j + added] = existing[j];
                    }
                }
                else
                {
                    current = range.RangeNodes[i];
                }

                updated[i] = current;
            }

            return new RangeMap(range.UpperBounds, updated);
        }

        [Pure]
        public static RangeMap RemoveNode(RangeMap range, int key, int toRemove, bool strict = true)
        {
            var index = range.GetRangeIndexForKey(key);
            var updated = new int[range.RangeNodes.Length][];
            for (var i = 0; i < range.RangeNodes.Length; i++)
            {
                int[] current;
                var existing = range.RangeNodes[i];
                if (i == index)
                {
                    // Copy the old nodes to a new array, skipping the removed node.
                    if (existing.Length > 0)
                    {
                        current = new int[existing.Length - 1];
                        var removed = 0;
                        for (var j = 0; j < existing.Length; j++)
                        {
                            if (existing[j] == toRemove)
                            {
                                removed = 1;
                                continue;
                            }

                            current[j - removed] = existing[j];
                        }

                        if (strict && removed == 0) throw new InvalidOperationException($"Cannot remove node {toRemove} from a set which does not contain it");
                    }
                    else
                    {
                        if (strict) throw new InvalidOperationException("Cannot remove a node from an empty set");
                        current = existing;
                    }
                }
                else
                {
                    current = existing;
                }

                updated[i] = current;
            }

            return new RangeMap(range.UpperBounds, updated);
        }

        [Pure]
        public int[] GetNodesForKey(int key) => this.RangeNodes[this.GetRangeIndexForKey(key)];

        [Pure]
        public int GetRangeIndexForKey(int key)
        {
            for (var i = 0; i < this.UpperBounds.Length; i++)
            {
                if (key <= this.UpperBounds[i]) return i;
            }

            return this.UpperBounds.Length - 1;
        }
    }
}