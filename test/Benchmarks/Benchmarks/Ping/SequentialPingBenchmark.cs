using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Linq;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using Orleans.TestingHost;
using BenchmarkGrainInterfaces.Ping;
using Benchmarks.Serialization;

namespace Benchmarks.Ping
{
    public class SequentialPingBenchmarkConfig : ManualConfig
    {
        public SequentialPingBenchmarkConfig()
        {
            Add(Job.ShortRun);
            Add(new MemoryDiagnoser());
        }
    }

    [Config(typeof(SequentialPingBenchmarkConfig))]
    public class SequentialPingBenchmark : IDisposable 
    {
        private readonly TestCluster host;
        private readonly IPingGrain grain;

        public SequentialPingBenchmark()
        {
            var options = new TestClusterOptions();
            host = new TestCluster(options);
            host.Deploy();
            grain = host.Client.GetGrain<IPingGrain>(Guid.NewGuid().GetHashCode());
            grain.Run().GetAwaiter().GetResult();
        }
        
        [Benchmark]
        public Task Ping() => grain.Run();
        
        public void Dispose()
        {
            host.StopAllSilos();
        }
    }
}