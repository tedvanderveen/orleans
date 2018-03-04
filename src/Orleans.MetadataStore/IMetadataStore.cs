using System.Threading.Tasks;

namespace Orleans.MetadataStore
{
    public interface IMetadataStore
    {
        Task<UpdateResult<TValue>> TryUpdate<TValue>(string key, TValue updated) where TValue : class, IVersioned;
        Task<ReadResult<TValue>> TryGet<TValue>(string key) where TValue : class, IVersioned;
    }
}