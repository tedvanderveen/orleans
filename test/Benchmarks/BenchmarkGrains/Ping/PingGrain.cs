using Orleans;
using BenchmarkGrainInterfaces.Ping;
using System.Threading.Tasks;

namespace BenchmarkGrains.Ping
{
    public class PingGrain : Grain, IPingGrain
    {
        private IPingGrain self;
        private IPingGrain higher;
        private IPingGrain lower;

        public override Task OnActivateAsync()
        {
            this.higher = this.GrainFactory.GetGrain<IPingGrain>(this.GetPrimaryKeyLong() + 1);
            this.lower = this.GrainFactory.GetGrain<IPingGrain>(this.GetPrimaryKeyLong() - 1);
            this.self = this.AsReference<IPingGrain>();
            return base.OnActivateAsync();
        }

        public Task Run()
        {
            return Task.CompletedTask;
        }

        public Task PingPongInterleave(IPingGrain other, int count)
        {
            if (count == 0) return Task.CompletedTask;
            return other.PingPongInterleave(this.self, count - 1);
        }

        public Task PingPongHigher(int count)
        {
            if (count == 0) return Task.CompletedTask;
            return higher.PingPongLower(count - 1);
        }

        public Task PingPongLower(int count)
        {
            if (count == 0) return Task.CompletedTask;
            return lower.PingPongHigher(count - 1);
        }
    }
}
