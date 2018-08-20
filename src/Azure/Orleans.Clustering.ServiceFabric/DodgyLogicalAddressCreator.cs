using System;
using System.Net;
using Orleans.Clustering.ServiceFabric.Models;
using Orleans.Runtime;

namespace Orleans.Clustering.ServiceFabric
{
    internal static class DodgyLogicalAddressCreator
    {
        public static int ConvertToId(SiloAddress address)
        {
            var hashCode = BitConverter.ToInt32(address.Endpoint.Address.GetAddressBytes(), 0);
            return hashCode;
        }

        public static int ConvertToId(IResolvedServicePartition silos)
        {
            return silos.Id.GetHashCode();
        }

        public static SiloAddress ConvertToLogicalAddress(ServicePartitionSilos silos)
        {
            var hashCode = (uint)silos.Partition.Id.GetHashCode();
            return SiloAddress.New(new IPEndPoint(hashCode, 65533), silos.Silos[0].SiloAddress.Generation);
        }
    }
}