using System;

namespace Orleans.ServiceFabric.Actors.Runtime
{
    internal class ActorTimer : IActorTimer
    {
        private readonly IDisposable timer;

        public ActorTimer(TimeSpan dueTime, TimeSpan period, IDisposable timer)
        {
            this.DueTime = dueTime;
            this.Period = period;
            this.timer = timer;
        }

        public void Dispose() => this.timer?.Dispose();

        public TimeSpan DueTime { get; }
        public TimeSpan Period { get; }
    }
}