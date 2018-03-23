using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace Tester.StreamingTests
{
    //one lease manager grain per stream provider, so its key is stream provider name
    public interface ILeaseManagerGrain : IGrainWithStringKey
    {
        Task<QueueId> Acquire();
        Task<bool> Renew(QueueId leaseNumber);
        Task Release(QueueId leaseNumber);
        Task<int> GetLeaseResposibility();
        Task SetQueuesAsLeases(IEnumerable<QueueId> queues);
        //methods used in test asserts
        Task RecordBalancerResponsibility(string balancerId, int ownedQueues);
        Task<Dictionary<string, int>> GetResponsibilityMap();

    }

    public class LeaseManagerGrain : Grain, ILeaseManagerGrain
    {
        //queueId is the lease id here
        private static readonly DateTime UnAssignedLeaseTime = DateTime.MinValue;
        private readonly int expectedSiloCount;
        private Dictionary<QueueId, DateTime> queueLeaseToRenewTimeMap;

        public LeaseManagerGrain(IConfiguration configuration)
        {
            this.expectedSiloCount = int.Parse(configuration["InitialSilosCount"]);
        }

        public override Task OnActivateAsync()
        {
            this.logger = this.GetLogger(this.GetGrainIdentity() +
                                         " / " +
                                         this.GetPrimaryKeyString() +
                                         " on " +
                                         this.ServiceProvider.GetRequiredService<ILocalSiloDetails>().SiloAddress);
            this.logger.Info("Activating");
            this.queueLeaseToRenewTimeMap = new Dictionary<QueueId, DateTime>();
            this.responsibilityMap = new Dictionary<string, int>();
            this.RegisterTimer(_ =>
                {
                    this.logger.Info("Still alive");
                    return Task.CompletedTask;
                },
                null,
                TimeSpan.FromMilliseconds(500),
                TimeSpan.FromMilliseconds(500));
            return Task.CompletedTask;
        }

        public override Task OnDeactivateAsync()
        {
            this.logger.Info("Deactivating");
            return Task.CompletedTask;
        }

        public Task<int> GetLeaseResposibility()
        {
            var resposibity = this.queueLeaseToRenewTimeMap.Count / this.expectedSiloCount;
            return Task.FromResult(resposibity);
        }

        public Task<QueueId> Acquire()
        {
            foreach (var lease in this.queueLeaseToRenewTimeMap)
            {
                //find the first unassigned lease and assign it
                if (lease.Value.Equals(UnAssignedLeaseTime))
                {
                    this.queueLeaseToRenewTimeMap[lease.Key] = DateTime.UtcNow;
                    return Task.FromResult(lease.Key);
                }
            }
            throw new KeyNotFoundException("No more lease to aquire");
        }

        public Task<bool> Renew(QueueId leaseNumber)
        {
            if (this.queueLeaseToRenewTimeMap.ContainsKey(leaseNumber))
            {
                this.queueLeaseToRenewTimeMap[leaseNumber] = DateTime.UtcNow;
                return Task.FromResult(true);
            }
            return Task.FromResult(false);
        }

        public Task Release(QueueId leaseNumber)
        {
            if (this.queueLeaseToRenewTimeMap.ContainsKey(leaseNumber))
                this.queueLeaseToRenewTimeMap[leaseNumber] = UnAssignedLeaseTime;
            return Task.CompletedTask;
        }

        public Task SetQueuesAsLeases(IEnumerable<QueueId> queueIds)
        {
            //if already set up, return
            if (this.queueLeaseToRenewTimeMap.Count > 0)
                return Task.CompletedTask;
            //set up initial lease map
            foreach (var queueId in queueIds)
            {
                this.queueLeaseToRenewTimeMap.Add(queueId, UnAssignedLeaseTime);
            }
            return Task.CompletedTask;
        }

        //methods used in test asserts
        private Dictionary<string, int> responsibilityMap;
        private Logger logger;

        public Task RecordBalancerResponsibility(string balancerId, int ownedQueues)
        {
            responsibilityMap[balancerId] = ownedQueues;
            this.logger.Info($"RecordBalancerResponsibility({balancerId}, {ownedQueues}) => \n{string.Join("\n", this.responsibilityMap.Select(kvp => $"{kvp.Key} = {kvp.Value}"))}");
            return Task.CompletedTask;
        }

        public Task<Dictionary<string, int>> GetResponsibilityMap()
        {
            this.logger.Info("GetResponsibilityMap");
            return Task.FromResult(responsibilityMap);
        }
    }
}
