using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace Orleans.ServiceFabric.Actors.Runtime
{
    [Serializable]
    public class ActorState : ISerializable
    {
        public Dictionary<string, object> Values { get; private set; } = new Dictionary<string, object>();

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(nameof(this.Values), this.Values);
        }
    }

    /// <summary>
    /// Represents the base class for actors.
    /// </summary>
    /// <remarks>
    /// The base type for actors, that provides the common functionality
    /// for actors that derive from <see cref="Actor"/>.
    /// The state is preserved across actor garbage collections and fail-overs.
    /// The storage and retrieval of the state is provided by the actor state provider. See 
    /// <see cref="IActorStateProvider"/> for more information.
    /// </remarks>
    /// <seealso cref="Actor"/>
    public abstract class ActorBase : Grain<ActorState>, IGrainCallFilter
    {
        private readonly ActorId actorId;

        internal ActorBase(ActorId actorId)
        {
            this.actorId = actorId;
        }

        /// <summary>
        /// Gets the identity of this actor with the actor service.
        /// </summary>
        /// <value>The <see cref="ActorId"/> for the actor.</value>
        public ActorId Id
        {
            get { return this.actorId; }
        }

        /// <summary>
        /// Gets the name of the application that contains the actor service that is hosting this actor.
        /// </summary>
        /// <value>The name of application that contains the actor service that is hosting this actor.</value>
        public string ApplicationName => throw new NotSupportedException($"{nameof(this.ApplicationName)} is not supported.");

        /// <summary>
        /// Gets the URI of the actor service that is hosting this actor.
        /// </summary>
        /// <value>The <see cref="System.Uri"/> of the actor service that is hosting this actor.</value>
        public Uri ServiceUri => throw new NotSupportedException($"{nameof(this.ServiceUri)} is not supported.");
        
        /// <summary>
        /// Override this method for performing any actions prior to an actor method is invoked.
        /// This method is invoked by actor runtime just before invoking an actor method.
        /// </summary>
        /// <param name="actorMethodContext">
        /// An <see cref="ActorMethodContext"/> describing the method that will be invoked by actor runtime after this method finishes.
        /// </param>
        /// <returns>
        /// Returns a <see cref="Task">Task</see> representing pre-actor-method operation.
        /// </returns>
        /// <remarks>
        /// This method is invoked by actor runtime prior to:
        /// <list type="bullet">
        /// <item><description>Invoking an actor interface method when a client request comes.</description></item>
        /// <item><description>Invoking a method on <see cref="IRemindable"/> interface when a reminder fires.</description></item>
        /// <item><description>Invoking a timer callback when timer fires.</description></item>
        /// </list>
        /// </remarks>
        protected virtual Task OnPreActorMethodAsync(ActorMethodContext actorMethodContext) => Task.CompletedTask;

        /// <summary>
        /// Override this method for performing any actions after an actor method has finished execution.
        /// This method is invoked by actor runtime an actor method has finished execution.
        /// </summary>
        /// <param name="actorMethodContext">
        /// An <see cref="ActorMethodContext"/> describing the method that was invoked by actor runtime prior to this method.
        /// </param>
        /// <returns>
        /// Returns a <see cref="Task">Task</see> representing post-actor-method operation.
        /// </returns>
        /// /// <remarks>
        /// This method is invoked by actor runtime prior to:
        /// <list type="bullet">
        /// <item><description>Invoking an actor interface method when a client request comes.</description></item>
        /// <item><description>Invoking a method on <see cref="IRemindable"/> interface when a reminder fires.</description></item>
        /// <item><description>Invoking a timer callback when timer fires.</description></item>
        /// </list>
        /// </remarks>
        protected virtual Task OnPostActorMethodAsync(ActorMethodContext actorMethodContext) => Task.CompletedTask;
        
        /// <summary>
        /// Gets the event for the specified event interface.
        /// </summary>
        /// <typeparam name="TEvent">The Event interface type.</typeparam>
        /// <returns>Returns an Event that represents the specified interface.</returns>
        protected TEvent GetEvent<TEvent>()
        {
            throw new NotSupportedException($"{nameof(this.GetEvent)} is not supported.");
            //return this.Manager.GetEvent<TEvent>(this.Id);
        }
        
        public async Task Invoke(IGrainCallContext context)
        {
            var method = context.Method;
            var callContext = ActorMethodContext.CreateForActor(method.Name);
            await this.OnPreActorMethodAsync(callContext);
            await context.Invoke();
            await this.OnPostActorMethodAsync(callContext);
        }
    }
}