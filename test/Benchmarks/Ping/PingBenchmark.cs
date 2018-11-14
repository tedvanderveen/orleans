using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.Threading;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using BenchmarkGrainInterfaces.Ping;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;

namespace Benchmarks.Ping
{
    [EtwProfiler(performExtraBenchmarksRun: false)]
    [Config(typeof(Config))]
    public class PingBenchmark
    {
        private ISiloHost host;
        private IClusterClient client;

        public class Config : ManualConfig
        {
            public Config() =>
                this.Add(new Job
                {
                    Run = {
                        LaunchCount = 1,
                        IterationCount = 2,
                        WarmupCount = 0
                    }
                });
        }

        [GlobalSetup]
        public void Setup()
        {
            this.host = new SiloHostBuilder().UseLocalhostClustering().Configure<ClusterOptions>(options => options.ClusterId = options.ServiceId = "dev").Build();
            this.host.StartAsync().GetAwaiter().GetResult();

            this.client = new ClientBuilder().UseLocalhostClustering().Configure<ClusterOptions>(options => options.ClusterId = options.ServiceId = "dev").Build();
            this.client.Connect().GetAwaiter().GetResult();
        }

        [Params(100, 1000, 10000)] public int Concurrency;
        
        public int Total = 1_000_000;

        public int Runners = 10;

        public async Task RunAsync()
        {
            Console.WriteLine($"Cold Run - 10000 concurrent.");
            await this.FullRunAsync(10000);
            Console.WriteLine($"Warm Run - 100 concurrent.");
            await this.FullRunAsync(100);
            Console.WriteLine($"Warm Run - 1000 concurrent.");
            await this.FullRunAsync(1000);
            Console.WriteLine($"Warm Run - 10000 concurrent.");
            await this.FullRunAsync(10000);
        }

        [Benchmark]
        public Task ConcurrentPing() => this.FullRunAsync(this.Runners, this.Total, this.Concurrency);

        private Task FullRunAsync(int concurrency) => this.FullRunAsync(this.Runners, this.Total, concurrency);

        private async Task FullRunAsync(int runners, int total, int concurrent)
        {
            var concurrentPerRunner = concurrent / runners;
            var totalPerRunner = total / runners;
            Console.WriteLine($"Starting {runners} runners each processing {total} tasks with a concurrency of {concurrent}");
            var reports = await Task.WhenAll(Enumerable.Range(0, runners).Select(runNumber => this.RunAsync(runNumber, totalPerRunner, concurrentPerRunner)));
            var finalReport = AggregateReports(reports);
            Console.WriteLine($"{finalReport.Succeeded} calls in {finalReport.Elapsed.TotalMilliseconds}ms.");
            Console.WriteLine($"{(int)(finalReport.Succeeded * 1000 / finalReport.Elapsed.TotalMilliseconds)} calls per second.");
            Console.WriteLine($"{finalReport.Failed} calls failed.");
        }

        private static Report AggregateReports(Report[] reports)
        {
            var finalReport = new Report();
            foreach (var report in reports)
            {
                finalReport.Succeeded += report.Succeeded;
                finalReport.Failed += report.Failed;
                finalReport.Elapsed =
                    TimeSpan.FromMilliseconds(Math.Max(finalReport.Elapsed.TotalMilliseconds,
                        report.Elapsed.TotalMilliseconds));
            }

            return finalReport;
        }

        public async Task<Report> RunAsync(int runNumber, int total, int concurrency)
        {
            var load = this.client.GetGrain<ILoadGrain>(Guid.NewGuid());
            await load.Generate(runNumber, total, concurrency);

            Report result;
            while (true)
            {
                result = await load.TryGetReport();
                if (result != null) break;
                await Task.Delay(TimeSpan.FromMilliseconds(100));
            }

            return result;
        }

        [GlobalCleanup]
        public void Teardown()
        {
            this.client.Dispose();
            this.host.Dispose();
        }
    }
}