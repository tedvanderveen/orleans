using System;
using System.Threading.Tasks;
using GrainInterfaces;
using Orleans;
using Orleans.Streams;

namespace Grains
{
    public class CalculatorGrain : Grain, ICalculatorGrain
    {
        private readonly ObserverSubscriptionManager<ICalculatorObserver> observers = new ObserverSubscriptionManager<ICalculatorObserver>();
        private double current;
        
        public async Task<double> Add(double value)
        {
            var result = this.current += value;
            await this.NotifySubscribers();
            return result;
        }

        public async Task<double> Divide(double value)
        {
            var result = this.current /= value;
            await this.NotifySubscribers();
            return result;
        }

        public Task<double> Get()
        {
            return Task.FromResult(current);
        }

        public async Task<double> Multiply(double value)
        {
            var result = current *= value;
            await this.NotifySubscribers();
            return result;
        }

        public async Task<double> Set(double value)
        {
            var result = current = value;
            await this.NotifySubscribers();
            return result;
        }

        public async Task<double> Subtract(double value)
        {
            var result = this.current -= value;
            await this.NotifySubscribers();
            return result;
        }

        public Task Subscribe(ICalculatorObserver observer)
        {
            if (!this.observers.IsSubscribed(observer)) observers.Subscribe(observer);
            return Task.FromResult(0);
        }

        private Task NotifySubscribers()
        {
            this.observers.Notify(observer => observer.CalculationUpdated(this.current));
            return this.GetStreamProvider("default").GetStream<double>(Guid.Empty, "updates").OnNextAsync(this.current);
        }
    }
}
