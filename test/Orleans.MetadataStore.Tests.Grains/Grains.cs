using System;
using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Orleans.MetadataStore.Tests
{       
    public interface IMetadataStoreGrain : IGrainWithGuidKey
    {
        Task<ReadResult<TValue>> Get<TValue>(string key) where TValue : class, IVersioned;
        Task<UpdateResult<TValue>> TryUpdate<TValue>(string key, TValue updated) where TValue : class, IVersioned;
    }

    public class MetadataStoreGrain : Grain, IMetadataStoreGrain
    {
        private readonly IMetadataStore store;
        public MetadataStoreGrain(IMetadataStore store)
        {
            this.store = store;
        }

        public Task<ReadResult<TValue>> Get<TValue>(string key) where TValue : class, IVersioned => this.store.TryGet<TValue>(key);

        public Task<UpdateResult<TValue>> TryUpdate<TValue>(string key, TValue updated) where TValue : class, IVersioned => this.store.TryUpdate(key, updated);
    }
    
    [Immutable]
    [Serializable]
    public class MyVersionedData : IVersioned
    {
        public string Value { get; set; }
        public long Version { get; set; }

        public override string ToString()
        {
            return $"{nameof(Version)}: {Version}, {nameof(Value)}: {Value}";
        }
    }
}