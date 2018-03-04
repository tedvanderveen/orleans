namespace Orleans.MetadataStore
{
    /// <summary>
    /// Combines <see cref="ReplicaSetConfiguration"/> with a corresponding set of references to store instances on each node.
    /// </summary>
    public class ExpandedReplicaSetConfiguration
    {
        public ExpandedReplicaSetConfiguration(
            ReplicaSetConfiguration configuration,
            IRemoteMetadataStore[][] storeReferences)
        {
            Configuration = configuration;
            StoreReferences = storeReferences;
        }

        public IRemoteMetadataStore[][] StoreReferences { get; }
        public ReplicaSetConfiguration Configuration { get; }

        public static ExpandedReplicaSetConfiguration Create(
            ReplicaSetConfiguration config,
            MetadataStoreOptions options,
            IStoreReferenceFactory factory)
        {
            IRemoteMetadataStore[][] refs;
            if (config?.Nodes != null)
            {
                refs = new IRemoteMetadataStore[config.Nodes.Length][];
                for (var i = 0; i < config.Nodes.Length; i++)
                {
                    var instances = refs[i] = new IRemoteMetadataStore[options.InstancesPerSilo];

                    for (short j = 0; j < options.InstancesPerSilo; j++)
                    {
                        instances[j] = factory.GetReference(config.Nodes[i], j);
                    }
                }
            }
            else
            {
                refs = null;
            }

            return new ExpandedReplicaSetConfiguration(config, refs);
        }
    }
}