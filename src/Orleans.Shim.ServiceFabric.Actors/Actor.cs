using System.Threading.Tasks;

namespace Orleans.ServiceFabric.Actors.Runtime
{
    /// <summary>
    /// Represents an actor that can have multiple reliable 'named' states associated with it.
    /// </summary>
    /// <remarks>
    /// The state is preserved across actor garbage collections and fail-overs. The storage and retrieval of the state is
    /// provided by the actor state provider <see cref="IActorStateProvider"/>.
    /// </remarks>
    /// <seealso cref="ActorBase"/>
    public abstract class Actor : ActorBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Actor"/> class.
        /// </summary>
        /// <param name="actorId">
        /// The <see cref="ActorId"/> for this actor instance.
        /// </param>
        protected Actor(ActorId actorId)
            : base(actorId)
        {
            this.StateManager = new ActorStateManager(this.State, this.WriteStateAsync);
        }

        /// <summary>
        /// Gets the state manager for <see cref="Actor"/>
        /// which can be used to get/add/update/remove named states.
        /// </summary>
        /// <value>
        /// An <see cref="IActorStateManager"/> which can be used to manage actor state.
        /// </value>
        public IActorStateManager StateManager { get; }

        /// <summary>
        /// Saves all the state changes (add/update/remove) that were made since last call to
        /// <see cref="Actor.SaveStateAsync"/>,
        /// to the actor state provider associated with the actor.
        /// </summary>
        /// <returns>A task that represents the asynchronous save operation.</returns>
        protected Task SaveStateAsync()
        {
            return this.WriteStateAsync();
        }
    }
}