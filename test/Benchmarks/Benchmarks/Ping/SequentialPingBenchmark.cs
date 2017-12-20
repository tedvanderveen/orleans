using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Linq;
using Orleans.TestingHost;
using BenchmarkGrainInterfaces.Ping;

namespace Benchmarks.Ping
{
    public class SequentialPingBenchmark
    {
        private TestCluster _host;

        public void Setup()
        {
            var options = new TestClusterOptions();
            _host = new TestCluster(options);
            _host.Deploy();
        }

        public async Task RunAsync()
        {
            Console.WriteLine("Cold Run.");
            await FullRunAsync(100);
            Console.WriteLine("Warm Run.");
            await FullRunAsync(1_000_000);
        }

        private async Task FullRunAsync(int numCalls)
        {
            var finalReport = new Report();
            var grain = _host.Client.GetGrain<IPingGrain>(Guid.NewGuid().GetHashCode());
            var stopwatch = Stopwatch.StartNew();
            for (var i = 0; i < numCalls; i++)
            {
                try
                {
                    await grain.Run();
                    finalReport.Succeeded++;
                }
                catch
                {
                    finalReport.Failed++;
                }
            }

            finalReport.Elapsed = stopwatch.Elapsed;

            Console.WriteLine($"{finalReport.Succeeded} calls in {finalReport.Elapsed.TotalMilliseconds}ms.");
            Console.WriteLine($"{finalReport.Succeeded * 1000 / finalReport.Elapsed.TotalMilliseconds} calls per second.");
            Console.WriteLine($"{finalReport.Failed} calls failed.");
        }

        public void Teardown()
        {
            _host.StopAllSilos();
        }
    }
}