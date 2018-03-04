/*
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.MetadataStore.Tests
{
    [TestCategory("MetadataStore")]
    public class MetadataStoreTests //: HostedTestClusterEnsureDefaultStarted
    {
        private readonly ITestOutputHelper output;

        public MetadataStoreTests(ITestOutputHelper output)//, DefaultClusterFixture fixture) : base(fixture)
        {
            this.output = output;
        }

        [Fact, TestCategory("BVT")]
        public async Task TryUpdate_SingleProposer()
        {
            var log = new XunitLogger(this.output, $"Client-{1}");

            var memoryStore = new MemoryLocalStore();
            var acceptors = Enumerable.Range(1, 3)
                .Select(i => (IAcceptor<int>)new Acceptor<int>(i.ToString(), memoryStore, new XunitLogger(this.output, $"Acceptor-{i}")));
            var proposer = new Proposer<int>(1.ToString(), new Ballot(0, 1), null, new XunitLogger(this.output, $"Proposer-{1}"));
            var config = new ReplicaSetConfiguration<int>(acceptors.ToArray(), new IProposer<int>[] { proposer }, 2, 2);
            await proposer.InstallConfiguration(config, CancellationToken.None);

            var (status, val) = await proposer.TryUpdate(1, (value, newValue) => value == newValue - 1 ? newValue : value, CancellationToken.None);

            Assert.Equal(OperationStatus.Success, status);
            Assert.Equal(1, val);
            log.LogInformation($"Updated value to {val}: {status}");

            (status, val) = await proposer.TryUpdate(0, (value, newValue) => value, CancellationToken.None);

            Assert.Equal(OperationStatus.Success, status);
            Assert.Equal(1, val);
            log.LogInformation($"Read value {val}: {status}");

            (status, val) = await proposer.TryUpdate(2, (value, newValue) => value == newValue - 1 ? newValue : value, CancellationToken.None);

            Assert.Equal(OperationStatus.Success, status);
            Assert.Equal(2, val);
            log.LogInformation($"Updated value to {val}: {status}");

            (status, val) = await proposer.TryUpdate(2, (value, newValue) => value == newValue - 1 ? newValue : value, CancellationToken.None);

            Assert.Equal(OperationStatus.Success, status);
            Assert.Equal(2, val);
            log.LogInformation("Failed to update value.");
        }

        [Fact, TestCategory("BVT")]
        public async Task TryUpdate_CompetingProposer_NonOverlapping()
        {
            var log = new XunitLogger(this.output, $"Client-{1}");

            var memoryStore = new MemoryLocalStore();
            var acceptors = Enumerable.Range(1, 3)
                .Select(i => (IAcceptor<int>)new Acceptor<int>(BitConverter.GetBytes(i), memoryStore, new XunitLogger(this.output, $"Acceptor-{i}")));
            var proposer1 = new Proposer<int>(new Ballot(0, 1), null, new XunitLogger(this.output, $"Proposer-{1}"));
            var proposer2 = new Proposer<int>(new Ballot(0, 2), null, new XunitLogger(this.output, $"Proposer-{2}"));
            var config = new ReplicaSetConfiguration<int>(acceptors.ToArray(), new IProposer<int>[] { proposer1, proposer2 }, 2, 2);
            await proposer1.InstallConfiguration(config, CancellationToken.None);
            await proposer2.InstallConfiguration(config, CancellationToken.None);

            var (status, val) = await proposer1.TryUpdate(1, (value, newValue) => value == newValue - 1 ? newValue : value, CancellationToken.None);

            Assert.Equal(OperationStatus.Success, status);
            Assert.Equal(1, val);
            log.LogInformation($"Updated value to {val}: {status}");

            (status, val) = await proposer1.TryUpdate(0, (value, newValue) => value, CancellationToken.None);

            Assert.Equal(OperationStatus.Success, status);
            Assert.Equal(1, val);
            log.LogInformation($"Read value {val}: {status}");

            (status, val) = await proposer2.TryUpdate(2, (value, newValue) => value == newValue - 1 ? newValue : value, CancellationToken.None);

            Assert.Equal(OperationStatus.Success, status);
            Assert.Equal(2, val);
            log.LogInformation($"Updated value to {val}: {status}");

            (status, val) = await proposer1.TryUpdate(3, (value, newValue) => value == newValue - 1 ? newValue : value, CancellationToken.None);

            Assert.Equal(OperationStatus.Success, status);
            Assert.Equal(3, val);
            log.LogInformation($"Updated value to {val}: {status}");
        }
    }
}
*/
