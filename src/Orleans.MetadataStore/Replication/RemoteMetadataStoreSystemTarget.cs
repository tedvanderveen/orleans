using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.MetadataStore.Storage;
using Orleans.Runtime;

namespace Orleans.MetadataStore
{
    internal class RemoteMetadataStoreSystemTarget : SystemTarget, IRemoteMetadataStore
    {
        private readonly MetadataStoreManager manager;
        private readonly ILocalStore store;

        public RemoteMetadataStoreSystemTarget(
            GrainId grainId,
            ISiloRuntimeClient runtimeClient,
            ILocalSiloDetails siloDetails,
            ILoggerFactory loggerFactory,
            MetadataStoreManager manager,
            ILocalStore store)
            : base(grainId, siloDetails.SiloAddress, loggerFactory)
        {
            this.RuntimeClient = runtimeClient;
            this.manager = manager;
            this.store = store;
        }

        public Task<PrepareResponse> Prepare(string key, Ballot proposerConfigBallot, Ballot ballot) => this.manager.Prepare(key, proposerConfigBallot, ballot);

        public Task<AcceptResponse> Accept(string key, Ballot proposerConfigBallot, Ballot ballot, object value) => this.manager.Accept(key, proposerConfigBallot, ballot, value);

        public Task<List<string>> GetKeys() => this.store.GetKeys(int.MaxValue);
    }
}