using System;
using System.Linq;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.MetadataStore
{
    [Immutable]
    [Serializable]
    public class ReplicaSetConfiguration : IVersioned
    {
        public ReplicaSetConfiguration(Ballot stamp, long version, SiloAddress[] nodes, int acceptQuorum, int prepareQuorum)
        {
            this.Stamp = stamp;
            this.Version = version;
            this.Nodes = nodes;
            this.AcceptQuorum = acceptQuorum;
            this.PrepareQuorum = prepareQuorum;
        }

        /// <summary>
        /// The addresses of all nodes.
        /// </summary>
        public SiloAddress[] Nodes { get; }

        /// <summary>
        /// The quorum size for Accept operations.
        /// </summary>
        public int AcceptQuorum { get; }

        /// <summary>
        /// The quorum size for Prepare operations.
        /// </summary>
        public int PrepareQuorum { get; }

        /// <summary>
        /// The unique ballot number of this configuration.
        /// </summary>
        public Ballot Stamp { get; }

        /// <summary>
        /// The monotonically increasing version number of this configuration.
        /// </summary>
        public long Version { get; }

        /// <inheritdoc />
        public override string ToString()
        {
            var nodes = this.Nodes == null ? "[]" : $"[{string.Join(", ", this.Nodes.Select(_ => _.ToString()))}]";
            return $"{nameof(Stamp)}: {Stamp}, {nameof(Version)}: {Version}, {nameof(Nodes)}: {nodes}, {nameof(AcceptQuorum)}: {AcceptQuorum}, {nameof(PrepareQuorum)}: {PrepareQuorum}";
        }
    }
}
