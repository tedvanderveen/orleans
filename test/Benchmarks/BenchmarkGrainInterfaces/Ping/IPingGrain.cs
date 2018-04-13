using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace BenchmarkGrainInterfaces.Ping
{
    public interface IPingGrain : IGrainWithIntegerKey
    {
        Task Run();

        [AlwaysInterleave]
        Task PingPongInterleave(IPingGrain other, int count);

        [AlwaysInterleave]
        Task PingPongHigher(int count);

        [AlwaysInterleave]
        Task PingPongLower(int count);
    }
}
