using System;
using System.Threading.Tasks;
using Orleans;

namespace BenchmarkGrainInterfaces.Ping
{
    public class Report
    {
        public long Succeeded { get; set; }
        public long Failed { get; set; }
        public TimeSpan Elapsed { get; set; }
    }

    public interface ILoadGrain : IGrainWithGuidKey
    {
        Task Generate(int runNumber, int total, int concurrency);
        Task<Report> TryGetReport();
    }
}
