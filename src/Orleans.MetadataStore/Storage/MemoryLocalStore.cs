using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Serialization;

namespace Orleans.MetadataStore.Storage
{
    public class MemoryLocalStore : ILocalStore
    {
        private readonly ConcurrentDictionary<string, object> lookup = new ConcurrentDictionary<string, object>();
        private readonly SerializationManager serializationManager;

        public MemoryLocalStore(SerializationManager serializationManager)
        {
            this.serializationManager = serializationManager;
        }

        public Task<TValue> Read<TValue>(string key)
        {
            if (this.lookup.TryGetValue(key, out var value))
            {
                return Task.FromResult((TValue)value);
            }

            return Task.FromResult(default(TValue));
        }

        public Task Write<TValue>(string key, TValue value)
        {
            this.lookup[key] = this.serializationManager.DeepCopy(value);
            return Task.CompletedTask;
        }

        public Task<List<string>> GetKeys(int maxResults = 100, string afterKey = null)
        {
            var include = afterKey == null;
            var results = new List<string>();
            foreach (var pair in this.lookup)
            {
                if (include)
                {
                    results.Add(pair.Key);
                }

                if (string.Equals(pair.Key, afterKey, StringComparison.Ordinal)) include = true;
                if (results.Count >= maxResults) break;
            }

            return Task.FromResult(results);
        }
    }
}
