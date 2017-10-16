using System;

namespace Microsoft.Orleans.ServiceFabric
{
    /// <summary>
    /// Options for Service Fabric cluster membership.
    /// </summary>
    internal class ServiceFabricMembershipOptions
    {
        /// <summary>
        /// Gets a value determining how long silos which are not currently registered in Service Fabric are allowed
        /// to exist before being considered dead.
        /// </summary>
        public TimeSpan UnknownSiloRemovalPeriod { get; set; } = TimeSpan.FromMinutes(1);
    }
}