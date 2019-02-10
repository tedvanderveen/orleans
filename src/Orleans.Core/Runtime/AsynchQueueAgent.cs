using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Threading;

namespace Orleans.Runtime
{
    internal abstract class AsynchQueueAgent<T> : AsynchAgent
    {
        private readonly QueueCounter queueCounter = new QueueCounter();

        protected AsynchQueueAgent(string nameSuffix, ExecutorService executorService, ILoggerFactory loggerFactory)
            : base(nameSuffix, executorService, loggerFactory)
        {
            ProcessAction = state => Process((T)state);
        }

        public WaitCallback ProcessAction { get; }

        public int Count => queueCounter.Count;

        public void QueueRequest(T request)
        {
            if (State != ThreadState.Running)
            {
                Log.LogWarning($"Invalid usage attempt of {Name} agent in {State.ToString()} state");
                return;
            }

            OnEnqueue(request);
            executor.QueueWorkItem(ProcessAction, request);
        }

        protected abstract void Process(T request);

        protected virtual bool DrainAfterCancel { get; } = false;

        protected virtual void OnEnqueue(T request)
        {
            queueCounter.Increment();
        }

        protected override ThreadPoolExecutorOptions.Builder ExecutorOptionsBuilder => base.ExecutorOptionsBuilder
            .WithDrainAfterCancel(DrainAfterCancel)
            .WithActionFilters(queueCounter);

        protected T GetWorkItemState(Threading.ExecutionContext context)
        {
            return (T)context.WorkItem.State;
        }

        private sealed class QueueCounter : ExecutionActionFilter
        {
            private int requestsInQueueCount;

            public int Count => requestsInQueueCount;

            public override void OnActionExecuting(Threading.ExecutionContext context)
            {
                Decrement();
            }

            public void Increment()
            {
                Interlocked.Increment(ref requestsInQueueCount);
            }

            public void Decrement()
            {
                Interlocked.Decrement(ref requestsInQueueCount);
            }
        }
    }


    internal abstract class ChannelAgent<T> : AsynchAgent
    {
        private readonly Channel<T> channel;
        private int count;

        protected ChannelAgent(string nameSuffix, ExecutorService executorService, ILoggerFactory loggerFactory)
            : base(nameSuffix, executorService, loggerFactory)
        {
            this.channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
        }

        public int Count => this.count;

        public void QueueRequest(T request)
        {
            if (State != ThreadState.Running)
            {
                Log.LogWarning($"Invalid usage attempt of {Name} agent in {State.ToString()} state");
                return;
            }

            if (this.channel.Writer.TryWrite(request)) Interlocked.Increment(ref this.count);
        }

        public sealed override void OnStart()
        {
            this.executor.QueueWorkItem(_ =>
            { 
                var reader = this.channel.Reader;
                var ct = this.Cts.Token;
                while (true)
                {
                    while (reader.TryRead(out var item))
                    {
                        Interlocked.Decrement(ref this.count);
                        this.Process(item);
                    }

                    var waitTask = reader.WaitToReadAsync(ct);
                    var more = waitTask.AsTask().GetAwaiter().GetResult();
                    if (!more)
                    {
                        this.Log.LogDebug("Drained all items in channel.");
                        return;
                    }
                }
            });
        }

        protected abstract void Process(T request);

        public override void Stop()
        {
            this.channel.Writer.TryComplete();
            base.Stop();
        }
    }
}
