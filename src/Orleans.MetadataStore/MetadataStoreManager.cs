using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.MetadataStore.Storage;
using Orleans.Runtime;
using Orleans.Runtime.Scheduler;

namespace Orleans.MetadataStore
{
    internal class MetadataStoreManager : IMetadataStore, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly ConcurrentDictionary<string, IAcceptor<IVersioned>> acceptors = new ConcurrentDictionary<string, IAcceptor<IVersioned>>();
        private readonly ConcurrentDictionary<string, IProposer<IVersioned>> proposers = new ConcurrentDictionary<string, IProposer<IVersioned>>();
        private readonly ChangeFunction<IVersioned, IVersioned> updateFunction = (current, updated) => (current?.Version ?? 0) == updated.Version - 1 ? updated : current;
        private readonly ChangeFunction<IVersioned, IVersioned> readFunction = (current, updated) => current;
        private readonly Func<string, IProposer<IVersioned>> proposerFactory;
        private readonly Func<string, IAcceptor<IVersioned>> acceptorFactory;
        private readonly OrleansTaskScheduler taskScheduler;
        private readonly ActivationDirectory activationDirectory;
        private readonly IServiceProvider serviceProvider;
        private readonly ConfigurationManager configurationManager;
        private readonly MetadataStoreOptions options;

        public MetadataStoreManager(
            OrleansTaskScheduler taskScheduler,
            ActivationDirectory activationDirectory,
            IServiceProvider serviceProvider,
            IOptions<MetadataStoreOptions> options,
            ConfigurationManager configurationManager,
            ILoggerFactory loggerFactory,
            ILocalStore localStore)
        {
            this.taskScheduler = taskScheduler;
            this.activationDirectory = activationDirectory;
            this.serviceProvider = serviceProvider;
            this.configurationManager = configurationManager;
            this.options = options.Value;
            this.configurationManager = configurationManager;

            // The configuration acceptor's stored configuration is used by itself as well as the storage proposers and acceptors.
            // Note that this configuration only becomes valid during startup, once the configuration acceptor is initialized.
            // The configuration proposer (initialized during startup) uses the configuration which is being proposed.
            ExpandedReplicaSetConfiguration GetAcceptedConfig() => this.configurationManager.AcceptedConfiguration;
            this.proposerFactory = key => new Proposer<IVersioned>(
                key,
                new Ballot(0, this.configurationManager.NodeId),
                GetAcceptedConfig,
                loggerFactory.CreateLogger($"MetadataStore.Proposer[{key}]"));

            Ballot GetAcceptedConfigBallot() => this.configurationManager.AcceptedConfiguration?.Configuration?.Stamp ?? Ballot.Zero;
            this.acceptorFactory = key => new Acceptor<IVersioned>(
                key,
                localStore,
                GetAcceptedConfigBallot,
                onUpdateState: null,
                log: loggerFactory.CreateLogger($"MetadataStore.Acceptor[{key}]"));

            // The cluster configuration is stored in a well-known key.
            this.acceptors[ConfigurationManager.ClusterConfigurationKey] = this.configurationManager;
            this.proposers[ConfigurationManager.ClusterConfigurationKey] = this.configurationManager;
        }
        
        public async Task<ReadResult<TValue>> TryGet<TValue>(string key) where TValue : class, IVersioned
        {
            var proposer = this.proposers.GetOrAdd(key, this.proposerFactory);
            var (status, value) = await proposer.TryUpdate(default(IVersioned), this.readFunction, CancellationToken.None);
            return new ReadResult<TValue>(status == ReplicationStatus.Success, (TValue)value);
        }

        public async Task<UpdateResult<TValue>> TryUpdate<TValue>(string key, TValue updated) where TValue : class, IVersioned
        {
            var proposer = this.proposers.GetOrAdd(key, this.proposerFactory);
            var (status, value) = await proposer.TryUpdate(updated, this.updateFunction, CancellationToken.None);
            return new UpdateResult<TValue>(status == ReplicationStatus.Success, (TValue)value);
        }

        public Task<PrepareResponse> Prepare(string key, Ballot proposerConfigBallot, Ballot ballot)
        {
            var acceptor = this.acceptors.GetOrAdd(key, this.acceptorFactory);
            return acceptor.Prepare(proposerConfigBallot, ballot);
        }

        public Task<AcceptResponse> Accept(string key, Ballot proposerConfigBallot, Ballot ballot, object value)
        {
            var acceptor = this.acceptors.GetOrAdd(key, this.acceptorFactory);
            return acceptor.Accept(proposerConfigBallot, ballot, (IVersioned)value);
        }

        void ILifecycleParticipant<ISiloLifecycle>.Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(nameof(MetadataStoreManager), ServiceLifecycleStage.RuntimeInitialize, this.OnRuntimeInitialize);
        }

        private async Task OnRuntimeInitialize(CancellationToken cancellationToken)
        {
            await this.configurationManager.Initialize();

            for (short i = 0; i < this.options.InstancesPerSilo; i++)
            {
                // Create a new store service worker instance.
                var grainId = GrainId.GetGrainServiceGrainId(i, Constants.KeyValueStoreSystemTargetTypeCode);
                var instance = ActivatorUtilities.CreateInstance<RemoteMetadataStoreSystemTarget>(this.serviceProvider, grainId);

                // Register the instance so that it can send and receive messages.
                this.taskScheduler.RegisterWorkContext(instance.SchedulingContext);
                this.activationDirectory.RecordNewSystemTarget(instance);
            }

            // TODO: bootstrap initial state from static file.
        }
    }
}