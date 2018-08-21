using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Orleans;
using Orleans.Clustering.ServiceFabric;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Hosting.ServiceFabric;

namespace StatefulCalculator
{
    public interface ICalculatorGrain : IGrainWithGuidKey
    {
        Task<int> Add(int value);
    }

    public class CalculatorGrain : Grain, ICalculatorGrain
    {
        private int accumulator;

        public Task<int> Add(int value)
        {
            this.accumulator += value;
            return Task.FromResult(this.accumulator);
        }
    }

    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class StatefulCalculator : StatefulService
    {
        private readonly TaskCompletionSource<IClusterClient> clientPromise = new TaskCompletionSource<IClusterClient>();

        public StatefulCalculator(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            var siloListener = OrleansServiceListener.CreateStateful((context, builder) =>
                {
                    builder.Configure<ClusterOptions>(options =>
                    {
                        // The service id is unique for the entire service over its lifetime. This is used to identify persistent state
                        // such as reminders and grain state.
                        options.ServiceId = context.ServiceName.ToString();

                        // The cluster id identifies a deployed cluster. Since Service Fabric uses rolling upgrades, the cluster id
                        // can be kept constant. This is used to identify which silos belong to a particular cluster.
                        options.ClusterId = "development";
                    });

                    // Service Fabric manages port allocations, so update the configuration using those ports.
                    // Gather configuration from Service Fabric.
                    var activation = context.CodePackageActivationContext;
                    var endpoints = activation.GetEndpoints();

                    // These endpoint names correspond to TCP endpoints specified in ServiceManifest.xml
                    var siloEndpoint = endpoints["OrleansSiloEndpoint"];
                    var gatewayEndpoint = endpoints["OrleansProxyEndpoint"];
                    var hostname = context.NodeContext.IPAddressOrFQDN;
                    builder.ConfigureEndpoints(hostname, siloEndpoint.Port, gatewayEndpoint.Port, listenOnAnyHostAddress: true);

                    builder.UseServiceFabricClustering(context);
                    builder.ConfigureLogging(logging =>
                    {
                        logging.SetMinimumLevel(LogLevel.Debug);
                        //logging.AddConsole();
                        logging.AddDebug();
                    });

                    // So that we can call into grains from RunAsync without messing about with TaskScheduler or ClientBuilder.
                    builder.EnableDirectClient();
                },
                host => this.clientPromise.TrySetResult(host.Services.GetRequiredService<IClusterClient>()));

            return new[] { siloListener };
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            var client = await clientPromise.Task;
            var log = client.ServiceProvider.GetRequiredService<ILogger<StatefulCalculator>>();
            var partitionKeyString = this.Partition.PartitionInfo.GetPartitionKeyString();
            var pid = Process.GetCurrentProcess().Id;
            var info = $"{pid} {partitionKeyString}";
            log.LogInformation("Inside RunAsync " + info);
            var grain = client.GetGrain<ICalculatorGrain>(Guid.Empty);

            //var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("myDictionary");

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var delayCancellation = new CancellationTokenSource();

                    var valueTask = grain.Add(1);
                    var task = await Task.WhenAny(valueTask, Task.Delay(TimeSpan.FromSeconds(5), delayCancellation.Token));
                    delayCancellation.Cancel();

                    if (task != valueTask)
                    {
                        log.LogError($"[{info}] Error: call took too long!");
                        continue;
                    }

                    var value = await valueTask;
                    log.LogInformation($"[{info}] value = {value}");
                }
                catch (Exception exception)
                {
                    log.LogWarning($"[{info}] Exception in RunAsync: {exception}");
                }

                //using (var tx = this.StateManager.CreateTransaction())
                //{
                //var result = await myDictionary.TryGetValueAsync(tx, "Counter");

                //ServiceEventSource.Current.ServiceMessage(this.Context, "Current Counter Value: {0}",
                //    result.HasValue ? result.Value.ToString() : "Value does not exist.");

                //await myDictionary.AddOrUpdateAsync(tx, "Counter", 0, (key, value) => ++value);

                // If an exception is thrown before calling CommitAsync, the transaction aborts, all changes are 
                // discarded, and nothing is saved to the secondary replicas.
                //await tx.CommitAsync();
                //}

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }

    internal static class FabricExtensions
    {
        /// <summary>
        /// Returns the partition key for the provided partition.
        /// </summary>
        /// <param name="partitionInformation">The partition.</param>
        /// <returns>The partition key for the provided partition.</returns>
        public static ServicePartitionKey GetPartitionKey(this ServicePartitionInformation partitionInformation)
        {
            switch (partitionInformation.Kind)
            {
                case ServicePartitionKind.Int64Range:
                    return new ServicePartitionKey(((Int64RangePartitionInformation)partitionInformation).LowKey);
                case ServicePartitionKind.Named:
                    return new ServicePartitionKey(((NamedPartitionInformation)partitionInformation).Name);
                case ServicePartitionKind.Singleton:
                    return ServicePartitionKey.Singleton;
                default:
                    throw new ArgumentOutOfRangeException(
                        nameof(partitionInformation),
                        $"Partition kind {partitionInformation.Kind} is not supported");
            }
        }

        /// <summary>
        /// Returns the partition key for the provided partition.
        /// </summary>
        /// <param name="partitionInformation">The partition.</param>
        /// <returns>The partition key for the provided partition.</returns>
        public static string GetPartitionKeyString(this ServicePartitionInformation partitionInformation)
        {
            var key = partitionInformation.GetPartitionKey();
            return $"{key.Kind}/{key.Value?.ToString() ?? "Singleton"}";
        }
    }
}
