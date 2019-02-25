using System;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;

namespace Orleans.Runtime.Messaging
{
#if true
    internal sealed class ConnectionMessageSender : IDisposable
    {
        internal static readonly object ContextItemKey = new object();
        private static readonly UnboundedChannelOptions ChannelOptions = new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        };

        private readonly Channel<Message> messages;
        private readonly ChannelWriter<Message> writer;
        private readonly CancellationTokenSource cancellation = new CancellationTokenSource();
        private readonly IMessageCenter messageCenter;
        private readonly IMessageSerializer serializer;
        private ConnectionContext connection;

        public ConnectionMessageSender(IMessageCenter messageCenter, IMessageSerializer messageSerializer)
        {
            this.messages = Channel.CreateUnbounded<Message>(ChannelOptions);
            this.writer = this.messages.Writer;
            this.messageCenter = messageCenter;
            this.serializer = messageSerializer;
        }

        public Task Run(ConnectionContext connection)
        {
            if (this.connection != null) throw new InvalidOperationException($"{nameof(ConnectionContext)} already set on this instance.");
            this.connection = connection;
            return Task.Run(this.Process);
        }

        public void Dispose() => this.Abort();

        public void Abort()
        {
            if (this.writer.TryComplete())
            {
                ThreadPool.UnsafeQueueUserWorkItem(cts => ((CancellationTokenSource)cts).Cancel(), this.cancellation);
            }
        }

        public void Send(Message message)
        {
            if (!this.writer.TryWrite(message))
            {
                this.RerouteMessage(message);
            }
        }

        private async Task Process()
        {
            var output = this.connection.Transport.Output;
            var reader = this.messages.Reader;
            try
            {
                while (!this.cancellation.IsCancellationRequested)
                {
                    var moreTask = reader.WaitToReadAsync();
                    var more = moreTask.IsCompleted ? moreTask.GetAwaiter().GetResult() : await moreTask;
                    if (!more)
                    {
                        break;
                    }

                    while (reader.TryRead(out var message))
                    {
                        this.serializer.Write(ref output, message);

                        var flushTask = output.FlushAsync();
                        var flushResult = flushTask.IsCompleted ? flushTask.GetAwaiter().GetResult() : await flushTask;
                        if (flushResult.IsCompleted || flushResult.IsCanceled) break;
                    }
                }
            }
            finally
            {
                while (reader.TryRead(out var message))
                {
                    this.RerouteMessage(message);
                }

                this.Abort();
                this.connection.Abort();
            }
        }

        private void RerouteMessage(Message message)
        {
            ThreadPool.UnsafeQueueUserWorkItem(msg => this.messageCenter.SendMessage((Message)msg), message);
        }
    }
#else
    internal sealed class ConnectionMessageSender : IDisposable
#if NETCOREAPP
        , IThreadPoolWorkItem
#endif
    {
        internal static readonly object ContextItemKey = new object();
#if !NETCOREAPP
        private static readonly WaitCallback ProcessWaitCallback = ctx => ((ConnectionMessageSender)ctx).Execute();
#endif
        private readonly ConcurrentBag<Message> items = new ConcurrentBag<Message>();
        private readonly IMessageCenter messageCenter;
        private readonly IMessageSerializer serializer;
        private readonly Action flushContinuation;
        private ValueTask<FlushResult> flushTask;
        private ConnectionContext connection;
        private int state;

        private const int STATE_IDLE = 0;
        private const int STATE_READY = 1;
        private const int STATE_RUNNING = 2;
        private const int STATE_MORE = 3;
        private const int STATE_COMPLETE = 4;
        private const int STATE_COMPLETE_MORE = 5;

        public ConnectionMessageSender(IMessageCenter messageCenter, IMessageSerializer messageSerializer)
        {
            this.messageCenter = messageCenter;
            this.serializer = messageSerializer;
            this.flushContinuation = () => this.ProcessFlush();
        }

        public void SetConnection(ConnectionContext connection)
        {
            if (this.connection != null) throw new InvalidOperationException($"{nameof(ConnectionContext)} already set on this instance.");
            this.connection = connection;
        }

        public void Dispose() => this.Abort();

        public void Abort()
        {
            var previousState = Interlocked.Exchange(ref this.state, STATE_COMPLETE_MORE);
            if (previousState == STATE_IDLE || previousState == STATE_COMPLETE)
            {
                this.ScheduleExecution();
            }
        }

        private void ScheduleExecution()
        {
#if NETCOREAPP
            ThreadPool.UnsafeQueueUserWorkItem(this, false);
#else
            ThreadPool.UnsafeQueueUserWorkItem(ProcessWaitCallback, this);
#endif
        }

        public void Send(Message message)
        {
            this.items.Add(message);
            
            // Transition the thread into an active state if it isn't already.
            int previousState;
            while ((previousState = Interlocked.CompareExchange(ref this.state, STATE_READY, STATE_IDLE)) != STATE_IDLE
                && previousState != STATE_READY
                && previousState != STATE_MORE
                && previousState != STATE_COMPLETE_MORE
                && (previousState = Interlocked.CompareExchange(ref this.state, STATE_MORE, STATE_RUNNING)) != STATE_RUNNING
                && (previousState = Interlocked.CompareExchange(ref this.state, STATE_COMPLETE_MORE, STATE_COMPLETE)) != STATE_COMPLETE)
            {
            }

            // If the previous state was an idle state then that means this call transitioned it into a ready state
            // and in that case it is responsible for scheduling the worker.
            // Otherwise, the state was either already running or ready to run and in either case the work just added
            // will be handled without further action by this call.
            if (previousState == STATE_IDLE || previousState == STATE_COMPLETE)
            {
                this.ScheduleExecution();
            }
        }

        public void Execute()
        {
            try
            {
                var previousState = Interlocked.CompareExchange(ref this.state, STATE_RUNNING, STATE_READY);
                if (previousState == STATE_COMPLETE_MORE)
                {
                    this.ProcessComplete();
                    return;
                }

                var con = this.connection;
                if (con == null)
                {
                    this.ScheduleExecution();
                    return;
                }

                var output = con.Transport.Output;
                while (true)
                {
                    var didRead = false;

                    while (items.TryTake(out var message))
                    {
                        this.serializer.Write(ref output, message);
                        didRead = true;
                    }

                    if (didRead)
                    {
                        this.flushTask = output.FlushAsync();
                        if (this.flushTask.IsCompleted)
                        {
                            if (this.ProcessFlush()) continue;
                            return;
                        }
                        else
                        {
                            this.flushTask.GetAwaiter().UnsafeOnCompleted(this.flushContinuation);
                        }
                    }
                    else
                    {
                        // If the thread is in the running state, transition to idle and return.
                        // If the thread is in a completion state, complete and return.
                        // If more work is requested, continue the loop.
                        while ((previousState = Interlocked.CompareExchange(ref this.state, STATE_IDLE, STATE_RUNNING)) != STATE_RUNNING
                            && previousState != STATE_MORE
                            && previousState != STATE_COMPLETE_MORE) { }

                        // If more work was queued up, continue, otherwise return.
                        if (previousState == STATE_RUNNING) return;
                        if (previousState == STATE_MORE) continue;
                        if (previousState == STATE_COMPLETE_MORE) this.ProcessComplete();
                        return;
                    }
                }
            }
            catch
            {
                Interlocked.Exchange(ref this.state, STATE_COMPLETE_MORE);
                this.ProcessComplete();
            }
        }

        private bool ProcessFlush()
        {
            var flushResult = flushTask.GetAwaiter().GetResult();
            if (flushResult.IsCompleted || flushResult.IsCanceled)
            {
                Interlocked.Exchange(ref this.state, STATE_COMPLETE_MORE);
                this.ProcessComplete();
                return false;
            }

            return true;
        }

        private void ProcessComplete()
        {
            do
            {
                while (items.TryTake(out var message))
                {
                    this.RerouteMessage(message);
                }
            }
            while (Interlocked.CompareExchange(ref this.state, STATE_COMPLETE, STATE_COMPLETE_MORE) != STATE_COMPLETE);

            this.Abort();
            this.connection.Abort();
        }


        private void RerouteMessage(Message message)
        {
            ThreadPool.UnsafeQueueUserWorkItem(msg => this.messageCenter.SendMessage((Message)msg), message);
        }
    }
#endif
}
