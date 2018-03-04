using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.MetadataStore.Storage;

namespace Orleans.Hosting
{
    public static class SimpleFileSystemStoreExtensions
    {
        public static ISiloHostBuilder UseSimpleFileSystemStore(this ISiloHostBuilder builder, Action<SimpleFileSystemStoreOptions> configure)
        {
            return builder.UseSimpleFileSystemStore(ob => ob.Configure(configure));
        }

        public static ISiloHostBuilder UseSimpleFileSystemStore(this ISiloHostBuilder builder, Action<OptionsBuilder<SimpleFileSystemStoreOptions>> configure)
        {
            return builder.ConfigureServices(services =>
            {
                services.AddSingleton<ILocalStore, SimpleFileSystemStore>();
                services.AddSingleton<IConfigurationValidator, SimpleFileSystemStoreOptionsValidator>();
                configure(services.AddOptions<SimpleFileSystemStoreOptions>());
            });
        }
    }
}