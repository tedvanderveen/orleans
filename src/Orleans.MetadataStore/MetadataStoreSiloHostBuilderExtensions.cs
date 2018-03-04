using Microsoft.Extensions.DependencyInjection;
using Orleans.MetadataStore;
using Orleans.MetadataStore.Storage;
using Orleans.Runtime;

namespace Orleans.Hosting
{
    public static class MetadataStoreSiloHostBuilderExtensions
    {
        public static ISiloHostBuilder UseMemoryLocalStore(this ISiloHostBuilder builder)
        {
            return builder.ConfigureServices(services => services.AddSingleton<ILocalStore, MemoryLocalStore>());
        }

        public static ISiloHostBuilder AddMetadataStore(this ISiloHostBuilder builder)
        {
            return builder
                .ConfigureApplicationParts(parts => parts.AddFrameworkPart(typeof(IRemoteMetadataStore).Assembly))
                .ConfigureServices((context, services) =>
                {
                    if (context.Properties.TryGetValue(nameof(AddMetadataStore), out var _)) return;
                    context.Properties[nameof(AddMetadataStore)] = nameof(AddMetadataStore);

                    services.AddSingleton<IStoreReferenceFactory, StoreReferenceFactory>();
                    services.AddSingleton<ConfigurationManager>();
                    services.AddSingleton<MetadataStoreManager>();
                    services.Add(new ServiceDescriptor(
                        typeof(IMetadataStore),
                        sp => sp.GetRequiredService<MetadataStoreManager>(),
                        ServiceLifetime.Singleton));
                    services.Add(new ServiceDescriptor(
                        typeof(ILifecycleParticipant<ISiloLifecycle>),
                        sp => sp.GetRequiredService<MetadataStoreManager>(),
                        ServiceLifetime.Singleton));
                });
        }
    }
}
