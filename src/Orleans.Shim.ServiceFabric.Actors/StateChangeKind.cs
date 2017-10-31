using System.Collections.Generic;
using System.Threading;

namespace Orleans.ServiceFabric.Actors.Runtime
{
    /// <summary>
    /// Represents the kind of state change for an actor state when 
    /// <see cref="IActorStateProvider.SaveStateAsync(ActorId, IReadOnlyCollection{T}, CancellationToken)"/>
    /// saves changes to a set of actor states.
    /// </summary>
    public enum StateChangeKind
    {
        /// <summary>
        /// No change in state
        /// </summary>
        None = 0,

        /// <summary>
        /// The state needs to be added.
        /// </summary>
        Add = 1,

        /// <summary>
        /// The state needs to be updated.
        /// </summary>
        Update = 2,

        /// <summary>
        /// The state needs to be removed.
        /// </summary>
        Remove = 3
    }
}