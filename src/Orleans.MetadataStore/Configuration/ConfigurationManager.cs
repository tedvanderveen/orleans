using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.MetadataStore.Storage;
using Orleans.Runtime;
using AsyncEx = Nito.AsyncEx;

namespace Orleans.MetadataStore
{
    /// <summary>
    /// The Configuration Manager is responsible for coordinating configuration (cluster membership) changes
    /// across the cluster.
    /// </summary>
    /// <remarks>
    /// From a high level, cluster configuration is stored in a special-purpose shared register. The linearizability
    /// properties of this register are used to ensure that safety requirements of the system are not violated.
    /// In particular, at no point in time and under no situation 
    /// </remarks>
    public class ConfigurationManager : IProposer<IVersioned>, IAcceptor<IVersioned>
    {
        public const string ClusterConfigurationKey = "MDS.Config";
        private const string NodeIdKey = "MDS.NodeId";
        private readonly ILocalStore store;
        private readonly IStoreReferenceFactory referenceFactory;
        private readonly IServiceProvider serviceProvider;
        private readonly MetadataStoreOptions options;

        private readonly ChangeFunction<ReplicaSetConfiguration, IVersioned> updateFunction =
            (current, updated) => (current?.Version ?? 0) == updated.Version - 1 ? updated : current;
        private readonly ChangeFunction<SiloAddress, SiloAddress[]> addFunction;
        private readonly ChangeFunction<SiloAddress, SiloAddress[]> removeFunction;
        private readonly ChangeFunction<object, IVersioned> readFunction = (current, updated) => current;
        private readonly AsyncEx.AsyncLock updateLock = new AsyncEx.AsyncLock();
        private readonly Proposer<IVersioned> proposer;
        private readonly Acceptor<IVersioned> acceptor;
        private readonly ILogger<ConfigurationManager> log;

        public ConfigurationManager(
            ILocalStore store,
            ILoggerFactory loggerFactory,
            IStoreReferenceFactory referenceFactory,
            IOptions<MetadataStoreOptions> options,
            IServiceProvider serviceProvider)
        {
            this.store = store;
            this.referenceFactory = referenceFactory;
            this.serviceProvider = serviceProvider;
            this.log = loggerFactory.CreateLogger<ConfigurationManager>();
            this.options = options.Value;
            this.addFunction = this.AddServer;
            this.removeFunction = this.RemoveServer;

            this.acceptor = new Acceptor<IVersioned>(
                ClusterConfigurationKey,
                store,
                () => this.AcceptedConfiguration?.Configuration?.Stamp ?? Ballot.Zero,
                this.OnUpdateConfiguration,
                loggerFactory.CreateLogger("MetadataStore.ConfigAcceptor")
            );

            // The config proposer always uses the configuration which it's proposing.
            this.proposer = new Proposer<IVersioned>(
                ClusterConfigurationKey,
                Ballot.Zero,
                () => this.ProposedConfiguration ?? this.AcceptedConfiguration,
                loggerFactory.CreateLogger("MetadataStore.ConfigProposer")
            );
        }

        private void OnUpdateConfiguration(RegisterState<IVersioned> state)
        {
            this.AcceptedConfiguration = ExpandedReplicaSetConfiguration.Create((ReplicaSetConfiguration) state.Value, this.options, this.referenceFactory);
        }

        /// <summary>
        /// Returns the most recently accepted configuration. Note that this configuration may not be committed.
        /// </summary>
        public ExpandedReplicaSetConfiguration AcceptedConfiguration { get; private set; }
        
        /// <summary>
        /// Returns the most recently proposed configuration.
        /// </summary>
        public ExpandedReplicaSetConfiguration ProposedConfiguration { get; private set; }

        public int NodeId { get; set; }

        public async Task Initialize()
        {
            this.NodeId = await this.store.Read<int>(NodeIdKey);
            if (this.NodeId == 0)
            {
                this.NodeId = Math.Abs(Guid.NewGuid().GetHashCode());
                await this.store.Write(NodeIdKey, this.NodeId);
            }

            this.proposer.Ballot = new Ballot(0, this.NodeId);
            await this.acceptor.EnsureStateLoaded();
        }

        public Task ForceLocalConfiguration(ReplicaSetConfiguration configuration) => this.acceptor.ForceState(configuration);

        public Task<UpdateResult<ReplicaSetConfiguration>> TryAddServer(SiloAddress address) => this.ModifyConfiguration(this.addFunction, address);

        public Task<UpdateResult<ReplicaSetConfiguration>> TryRemoveServer(SiloAddress address) => this.ModifyConfiguration(this.removeFunction, address);

        private async Task<UpdateResult<ReplicaSetConfiguration>> ModifyConfiguration(ChangeFunction<SiloAddress, SiloAddress[]> changeFunc, SiloAddress address)
        {
            // Update the configuration using two consensus rounds, first reading/committing the existing configuration,
            // then modifying it to add or remove a single server and committing the new value.
            // 
            // Note that performing the update using a single consensus round could break the invariant that configuration
            // grows or shrinks by at most one node at a time. For example, consider a scenario in which a commit was only
            // accepted on one acceptor in a set before the proposer faulted. In that case, the configuration may be seen
            // by the hypothetical single read-modify-write consensus round before the majority of acceptors are using
            // that configuration. The effect is that the majority may see a configuration change which changes by two
            // or more nodes simultaneously.
            var cancellation = CancellationToken.None;
            using (await this.updateLock.LockAsync())
            {
                // Read the currently committed configuration, potentially committing a partially-committed configuration in the process.
                var (status, committedValue) = await this.proposer.TryUpdate(null, this.readFunction, cancellation);
                var committedConfig = (ReplicaSetConfiguration) committedValue;
                if (status != ReplicationStatus.Success)
                {
                    return new UpdateResult<ReplicaSetConfiguration>(false, committedConfig);
                }

                // Modify the replica set.
                var newNodes = changeFunc(committedConfig?.Nodes, address);
                if (newNodes == committedConfig?.Nodes)
                {
                    // The new address was already in the committed configuration, so no additional work needs to be done.
                    return new UpdateResult<ReplicaSetConfiguration>(true, committedConfig);
                }

                // Assemble the new configuration.
                var committedStamp = committedConfig?.Stamp ?? default(Ballot);
                this.proposer.Ballot = this.proposer.Ballot.AdvanceTo(committedStamp);
                var newStamp = this.proposer.Ballot.Successor();
                var quorum = newNodes.Length / 2 + 1;
                var committedVersion = committedValue?.Version ?? 0;
                var config = new ReplicaSetConfiguration(newStamp, committedVersion + 1, newNodes, quorum, quorum);
                this.ProposedConfiguration = ExpandedReplicaSetConfiguration.Create(config, this.options, this.referenceFactory);

                // Attempt to commit the new configuration.
                (status, committedValue) = await this.proposer.TryUpdate(config, this.updateFunction, cancellation);
                var success = status == ReplicationStatus.Success;

                // Ensure that a quorum of acceptors have the latest value for all keys.
                // This replicates all values to the 
                if (success) success = await this.CatchupAllAcceptors();

                return new UpdateResult<ReplicaSetConfiguration>(success, (ReplicaSetConfiguration) committedValue);
            }
        }

        private async Task<bool> CatchupAllAcceptors()
        {
            var (success, allKeys) = await GetAllKeys();
            if (success)
            {
                this.log.LogError($"Failed to successfully read keys froma quorum of nodes.");
                return false;
            }

            var storeManager = this.serviceProvider.GetRequiredService<IMetadataStore>();
            var batchTasks = new List<Task>(100);
            foreach (var batch in allKeys.BatchIEnumerable(100))
            {
                foreach (var key in batch)
                {
                    batchTasks.Add(storeManager.TryGet<IVersioned>(key));
                }

                await Task.WhenAll(batchTasks);
            }

            return true;
        }

        private async Task<(bool, HashSet<string>)> GetAllKeys()
        {
            var quorum = this.ProposedConfiguration.Configuration.PrepareQuorum;
            var remainingConfirmations = quorum;
            var storeReferences = this.ProposedConfiguration.StoreReferences;
            var allKeys = new HashSet<string>();
            foreach (var storeReference in storeReferences)
            {
                var remoteMetadataStore = storeReference[0];

                try
                {
                    var storeKeys = await remoteMetadataStore.GetKeys();

                    foreach (var key in storeKeys) allKeys.Add(key);
                    --remainingConfirmations;

                    if (remainingConfirmations == 0) break;
                }
                catch (Exception exception)
                {
                    this.log.LogWarning($"Exception calling {nameof(IRemoteMetadataStore.GetKeys)} on remote store {remoteMetadataStore}: {exception}");
                }
            }

            var success = remainingConfirmations > 0;
            return (success, allKeys);
        }

        private SiloAddress[] AddServer(SiloAddress[] existingNodes, SiloAddress nodeToAdd)
        {
            // Add the new node to the list of nodes, being sure not to add a duplicate.
            var newNodes = new SiloAddress[(existingNodes?.Length ?? 0) + 1];
            if (existingNodes != null)
            {
                for (var i = 0; i < existingNodes.Length; i++)
                {
                    // If the configuration already contains the specified node, return the already-confirmed configuration.
                    if (existingNodes[i].Equals(nodeToAdd))
                    {
                        return existingNodes;
                    }

                    newNodes[i] = existingNodes[i];
                }
            }

            // Add the new node at the end.
            newNodes[newNodes.Length - 1] = nodeToAdd;

            return newNodes;
        }

        private SiloAddress[] RemoveServer(SiloAddress[] existingNodes, SiloAddress nodeToRemove)
        {
            if (existingNodes == null || existingNodes.Length == 0) return existingNodes;

            // Remove the node from the list of nodes.
            var newNodes = new SiloAddress[existingNodes.Length - 1];
            var skipped = 0;
            for (var i = 0; i < existingNodes.Length; i++)
            {
                var current = existingNodes[i + skipped];

                // If the node is encountered, skip it.
                if (current.Equals(nodeToRemove))
                {
                    skipped = 1;
                    continue;
                }

                // If the array bound has been hit, then either the last element is the target
                // or the target is not present.
                if (i == newNodes.Length) break;

                newNodes[i] = current;
            }

            // If no nodes changed, return a reference to the original configuration.
            if (skipped == 0) return existingNodes;

            return newNodes;
        }

        Task<(ReplicationStatus, IVersioned)> IProposer<IVersioned>.TryUpdate<TArg>(
            TArg value,
            ChangeFunction<TArg, IVersioned> changeFunction,
            CancellationToken cancellationToken) =>
            this.proposer.TryUpdate(value, changeFunction, cancellationToken);

        Task<PrepareResponse> IAcceptor<IVersioned>.Prepare(Ballot proposerConfigBallot, Ballot ballot) =>
            this.acceptor.Prepare(proposerConfigBallot, ballot);

        Task<AcceptResponse> IAcceptor<IVersioned>.Accept(Ballot proposerConfigBallot, Ballot ballot, IVersioned value) =>
            this.acceptor.Accept(proposerConfigBallot, ballot, value);
    }
}
