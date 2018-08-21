using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Clustering.ServiceFabric.Models;
using Orleans.Runtime;

namespace Orleans.Clustering.ServiceFabric
{
    internal class FabricSiloEndpointResolver : IEndpointResolver, IFabricServiceStatusListener
    {
        private readonly IFabricServiceSiloResolver resolver;
        private ServicePartitionSilos[] cache = Array.Empty<ServicePartitionSilos>();

        public FabricSiloEndpointResolver(IFabricServiceSiloResolver resolver)
        {
            this.resolver = resolver;
            this.resolver.Subscribe(this);
        }

        public ValueTask<SiloAddress> ResolveEndpoint(SiloAddress address)
        {
            var targetId = DodgyLogicalAddressCreator.ConvertToId(address);
            var result = CacheLookup(targetId);
            if (result != null) return new ValueTask<SiloAddress>(result);

            return ResolveAsync(targetId);

            SiloAddress CacheLookup(int id)
            {
                foreach (var silo in this.cache)
                {
                    var thisId = DodgyLogicalAddressCreator.ConvertToId(silo.Partition);
                    if (thisId == id)
                    {
                        var silos = silo.Silos;
                        if (silos.Count == 0) return null;
                        return silos[0].SiloAddress;
                    }
                }

                return null;
            }

            async ValueTask<SiloAddress> ResolveAsync(int id)
            {
                await this.resolver.Refresh();

                var silo = CacheLookup(id);
                if (silo == null) throw new KeyNotFoundException($"SiloAddress {address} not found. Cached partitions: {this.cache.Length.ToString()}");
                return silo;
            }
        }

        public void OnUpdate(ServicePartitionSilos[] silos)
        {
            this.cache = silos ?? throw new ArgumentNullException(nameof(silos));
        }
    }
}
