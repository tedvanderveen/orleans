using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.MetadataStore.Storage;
using Orleans.Runtime;

namespace Orleans.MetadataStore
{
    public class LiteDBLocalStorage : ILocalStore, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly LiteDBLocalStorageOptions options;
        private LiteDatabase db;
        private LiteCollection<KeyValueDocument> collection;

        public LiteDBLocalStorage(IOptions<LiteDBLocalStorageOptions> options)
        {
            this.options = options.Value;
        }

        public Task<TValue> Read<TValue>(string key)
        {
            var document = this.collection.FindOne(doc => doc.Id == key);
            var value = document?.Value;
            if (value == null) return Task.FromResult(default(TValue));
            return Task.FromResult((TValue)value);
        }

        public Task Write<TValue>(string key, TValue value)
        {
            this.collection.Upsert(new KeyValueDocument { Id = key, Value = value });
            return Task.CompletedTask;
        }

        public Task<List<string>> GetKeys(int maxResults = 100, string afterKey = null)
        {
            var keys = this.collection
                .FindAll()
                .Select(doc => doc.Id)
                .Where(key => afterKey == null || string.CompareOrdinal(key, afterKey) > 0)
                .Take(maxResults)
                .ToList();
            return Task.FromResult(keys);
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(nameof(LiteDBLocalStorage), ServiceLifecycleStage.RuntimeInitialize - 1, this.Start, this.Stop);
        }

        private Task Start(CancellationToken arg)
        {
            this.db = new LiteDatabase(this.options.ConnectionString);
            this.collection = this.db.GetCollection<KeyValueDocument>(this.options.CollectionName);
            //this.collection.EnsureIndex("Key", true);
            return Task.CompletedTask;
        }

        private Task Stop(CancellationToken arg)
        {
            this.db.Dispose();
            return Task.CompletedTask;
        }
    }

    public class KeyValueDocument
    {
        public string Id { get; set; }

        public object Value { get; set; }
    }

    public class LiteDBLocalStorageOptions
    {
        public string ConnectionString { get; set; }

        public string CollectionName { get; set; } = "data";
    }

    public static class LiteDBExtensions
    {
        public static ISiloHostBuilder UseLiteDBLocalStore(this ISiloHostBuilder builder, Action<LiteDBLocalStorageOptions> configure)
        {
            return builder.UseLiteDBLocalStore(ob => ob.Configure(configure));
        }

        public static ISiloHostBuilder UseLiteDBLocalStore(this ISiloHostBuilder builder, Action<OptionsBuilder<LiteDBLocalStorageOptions>> configure)
        {
            return builder.ConfigureServices(services =>
            {
                services.AddSingleton<LiteDBLocalStorage>();
                services.Add(new ServiceDescriptor(typeof(ILocalStore), sp => sp.GetRequiredService<LiteDBLocalStorage>(), ServiceLifetime.Singleton));
                services.Add(new ServiceDescriptor(typeof(ILifecycleParticipant<ISiloLifecycle>), sp => sp.GetRequiredService<LiteDBLocalStorage>(), ServiceLifetime.Singleton));

                configure(services.AddOptions<LiteDBLocalStorageOptions>());
            });
        }
    }
}
