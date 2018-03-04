using System;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.MetadataStore.Ranges
{
    [Immutable]
    [Serializable]
    public class RangeConfiguration : IVersioned
    {
        public RangeConfiguration(Range range, SiloAddress[] replicas, long version, Ballot stamp)
        {
            Range = range;
            Replicas = replicas;
            Version = version;
            Stamp = stamp;
        }

        public SiloAddress[] Replicas { get; }
        
        public Range Range { get; }

        public long Version { get; }

        public Ballot Stamp { get; }
    }
}
