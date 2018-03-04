using Orleans.Runtime;

namespace Orleans.MetadataStore
{
    /// <summary>
    /// Functionality for converting between <see cref="SiloAddress"/> and <see cref="IRemoteMetadataStore"/> instances.
    /// </summary>
    public interface IStoreReferenceFactory
    {
        SiloAddress GetAddress(IRemoteMetadataStore remoteStore);
        IRemoteMetadataStore GetReference(SiloAddress address, short instanceNum);
    }
}