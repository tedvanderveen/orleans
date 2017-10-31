using System;

namespace Orleans.ServiceFabric.Actors.Runtime
{
    /// <summary>
    /// Represents the timer set on an Actor.
    /// </summary>
    public interface IActorTimer : IDisposable
    {
        /// <summary>
        /// Gets the time when timer is first due.
        /// </summary>
        /// <value>Time as <see cref="System.TimeSpan"/> when timer is first due.</value>
        TimeSpan DueTime { get; }

        /// <summary>
        /// Gets the periodic time when timer will be invoked.
        /// </summary>
        /// <value>Periodic time as <see cref="System.TimeSpan"/> when timer will be invoked.</value>
        TimeSpan Period { get; }
    }
}