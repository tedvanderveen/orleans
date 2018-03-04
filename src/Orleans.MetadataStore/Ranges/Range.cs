using System;
using System.Diagnostics.Contracts;
using Orleans.Concurrency;

namespace Orleans.MetadataStore.Ranges
{
    [Immutable]
    [Serializable]
    public struct Range
    {
        public Range(int minKey, int maxKey)
        {
            if (minKey > maxKey) ThrowMinGreaterThanMax();
            MinKey = minKey;
            MaxKey = maxKey;
        }

        public void Deconstruct(out int minKey, out int maxKey)
        {
            minKey = MinKey;
            maxKey = MaxKey;
        }

        public int MinKey { get; }

        public int MaxKey { get; }

        [Pure]
        public bool Includes(int key) => key >= this.MinKey && key <= this.MaxKey;

        [Pure]
        public Range Merge(Range other) => new Range(Math.Min(this.MinKey, other.MinKey), Math.Max(this.MaxKey, other.MaxKey));

        [Pure]
        public (Range, Range) Split(int boundary) => (new Range(this.MinKey, boundary), new Range(boundary + 1, this.MaxKey));

        private static void ThrowMinGreaterThanMax() => throw new ArgumentOutOfRangeException("Minimum argument cannot be greater than maximum argument");
    }
}