using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Runtime;

namespace Orleans.Hosting
{
    /// <summary>
    /// Extensions for hosting an Orleans silo using <see cref="Microsoft.Extensions.Hosting.IHostBuilder"/>.
    /// </summary>
    public static class HostBuilderExtensions
    {
        private const string SiloBuilderKey = "OrleansSiloBuilderInstance";

        /// <summary>
        /// Adds an Orleans silo to the host builder.
        /// </summary>
        /// <param name="hostBuilder">The host builder.</param>
        /// <param name="configure">The delegate used to configure Orleans.</param>
        /// <returns></returns>
        public static IHostBuilder AddOrleans(this IHostBuilder hostBuilder, Action<ISiloHostBuilder> configure)
        {
            var siloHostBuilder = GetSiloBuilder(hostBuilder);
            configure?.Invoke(siloHostBuilder);
            siloHostBuilder.ConfigureDefaults();
            hostBuilder.ConfigureServices((ctx, services) =>
            {
                siloHostBuilder.GetApplicationPartManager().ConfigureDefaults();
                services.AddSingleton<IHostedService, HostedSiloService>();
                services.AddOptions();
                services.AddLogging();
            });

            return hostBuilder;
        }

        private static ISiloHostBuilder GetSiloBuilder(IHostBuilder hostBuilder)
        {
            ISiloHostBuilder siloBuilder;
            if (hostBuilder.Properties.TryGetValue(SiloBuilderKey, out var value))
            {
                siloBuilder = value as ISiloHostBuilder;
                if (siloBuilder == null)
                    throw new InvalidOperationException($"{nameof(IHostBuilder)}.{nameof(IHostBuilder.Properties)}[\"{SiloBuilderKey}\"] must be an instance of {nameof(ISiloHostBuilder)}.");
            }
            else
            {
                hostBuilder.Properties[SiloBuilderKey] = siloBuilder = new NestedSiloBuilder(hostBuilder);
            }

            return siloBuilder;
        }

        private class NestedSiloBuilder : ISiloHostBuilder
        {
            private readonly IHostBuilder hostBuilder;

            public NestedSiloBuilder(IHostBuilder hostBuilder)
            {
                this.hostBuilder = hostBuilder;
            }

            public IDictionary<object, object> Properties => this.hostBuilder.Properties;

            public ISiloHost Build() => throw new NotSupportedException($"This is a nested silo builder, it can only be built as a part of the outer {nameof(IHostBuilder)} which contains it.");

            public ISiloHostBuilder ConfigureHostConfiguration(Action<IConfigurationBuilder> configureDelegate)
            {
                this.hostBuilder.ConfigureHostConfiguration(configureDelegate);
                return this;
            }

            public ISiloHostBuilder ConfigureAppConfiguration(Action<HostBuilderContext, IConfigurationBuilder> configureDelegate)
            {
                this.hostBuilder.ConfigureAppConfiguration((ctx, cb) => configureDelegate(GetContext(ctx), cb));
                return this;
            }

            public ISiloHostBuilder ConfigureServices(Action<HostBuilderContext, IServiceCollection> configureDelegate)
            {
                this.hostBuilder.ConfigureServices((ctx, serviceCollection) => configureDelegate(GetContext(ctx), serviceCollection));
                return this;
            }

            public ISiloHostBuilder UseServiceProviderFactory<TContainerBuilder>(IServiceProviderFactory<TContainerBuilder> factory)
            {
                this.hostBuilder.UseServiceProviderFactory(factory);
                return this;
            }

            public ISiloHostBuilder ConfigureContainer<TContainerBuilder>(Action<HostBuilderContext, TContainerBuilder> configureDelegate)
            {
                this.hostBuilder.ConfigureContainer<TContainerBuilder>((ctx, containerBuilder) => configureDelegate(GetContext(ctx), containerBuilder));
                return this;
            }

            private static HostBuilderContext GetContext(Microsoft.Extensions.Hosting.HostBuilderContext ctx)
            {
                var siloContext = new HostBuilderContext(ctx.Properties)
                {
                    Configuration = ctx.Configuration,
                    HostingEnvironment = new HostingEnvironment(ctx.HostingEnvironment)
                };
                return siloContext;
            }

            private class HostingEnvironment : IHostingEnvironment
            {
                private readonly Microsoft.Extensions.Hosting.IHostingEnvironment env;

                public HostingEnvironment(Microsoft.Extensions.Hosting.IHostingEnvironment env)
                {
                    this.env = env;
                }

                public string EnvironmentName
                {
                    get => this.env.EnvironmentName;
                    set => this.env.EnvironmentName = value;
                }

                public string ApplicationName
                {
                    get => this.env.ApplicationName;
                    set => this.env.ApplicationName = value;
                }
            }
        }

        [SuppressMessage("ReSharper", "ClassNeverInstantiated.Local", Justification = "Class is instantiated via dependency injection.")]
        private class HostedSiloService : IHostedService
        {
            private readonly Silo silo;
            private readonly IServiceProvider services;

            public HostedSiloService(Silo silo, IServiceProvider services)
            {
                this.silo = silo;
                this.services = services;
            }

            /// <inheritdoc cref="IHostedService.StartAsync" />
            public async Task StartAsync(CancellationToken cancellationToken)
            {
                var validators = this.services.GetService<IEnumerable<IConfigurationValidator>>();
                foreach (var validtor in validators)
                {
                    validtor.ValidateConfiguration();
                }

                await this.silo.StartAsync(cancellationToken).ConfigureAwait(false);
            }

            /// <inheritdoc cref="IHostedService.StopAsync" />
            public async Task StopAsync(CancellationToken cancellationToken)
            {
                await this.silo.StopAsync(cancellationToken).ConfigureAwait(false);
                await this.silo.SiloTerminated.ConfigureAwait(false);
            }
        }
    }
}