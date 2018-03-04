using System;
using Orleans.Runtime;

namespace Orleans.MetadataStore
{
    internal class StoreReferenceFactory : IStoreReferenceFactory
    {
        private readonly IInternalGrainFactory grainFactory;

        public StoreReferenceFactory(IInternalGrainFactory grainFactory)
        {
            this.grainFactory = grainFactory;
        }

        public SiloAddress GetAddress(IRemoteMetadataStore remoteStore)
        {
            if (remoteStore is GrainReference grainReference) return grainReference.SystemTargetSilo;
            throw new InvalidOperationException($"Object of type {remoteStore?.GetType()?.ToString() ?? "null"} is not an instance of type {typeof(GrainReference)}");
        }

        public IRemoteMetadataStore GetReference(SiloAddress address, short instanceNum)
        {
            var grainId = GrainId.GetGrainServiceGrainId(instanceNum, Constants.KeyValueStoreSystemTargetTypeCode);
            return this.grainFactory.GetSystemTarget<IRemoteMetadataStore>(grainId, address);
        }
    }
}