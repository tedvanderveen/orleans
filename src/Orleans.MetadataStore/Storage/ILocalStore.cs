using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.MetadataStore.Storage
{
    public interface ILocalStore
    {
        Task<TValue> Read<TValue>(string key);
        Task Write<TValue>(string key, TValue value);
        Task<List<string>> GetKeys(int maxResults = 100, string afterKey = null);
    }
}