using System;
using System.Threading.Tasks;
using GrainInterfaces;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;

namespace Grains
{
    [ImplicitStreamSubscription("updates")]
    public class CalculatorObserverGrain : Grain, ICalculatorObserverGrain, IAsyncObserver<double>
    {
        public override async Task OnActivateAsync()
        {
            await this.GetStreamProvider("default").GetStream<double>(this.GetPrimaryKey(), "updates").SubscribeAsync(this);
        }

        public Task OnNextAsync(double item, StreamSequenceToken token = null)
        {
            this.GetLogger().Info($"Observing calculation updated to {item}");
            return Task.CompletedTask;
        }

        public Task OnCompletedAsync() => Task.CompletedTask;

        public Task OnErrorAsync(Exception ex)
        {
            this.GetLogger().Error(ex.GetType().GetHashCode(), $"Error in updates stream: {ex}");
            return Task.CompletedTask;
        }
    }
}
