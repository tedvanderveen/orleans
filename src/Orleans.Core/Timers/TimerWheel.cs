using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Threading;
using ExecutionContext = System.Threading.ExecutionContext;

namespace Orleans.Timers
{
    public static class TimerManager
    {
        public static DelayAwaitable Delay(TimeSpan timeout)
        {
            var dueTime = DateTime.UtcNow + timeout;
            var result = new DelayAwaitable();
            var holder = new DelayTimerElementHolder(dueTime, result);
            TimerWheel<DelayAwaitable>.Register(holder);
            return result;
        }
    }

    // Removes the need in timer per item, optimized for single-threaded consumption. 
    internal static class TimerWheel<T> where T : IOnTimeout
    {
        [ThreadStatic]
        private static TimerWheelQueueThreadLocals<T> queueThreadLocals;

        private static readonly SparseArray<TimerWheelQueueThreadLocals<T>> AllThreadLocals = new SparseArray<TimerWheelQueueThreadLocals<T>>(16);

        private static readonly Timer QueueChecker;

        static TimerWheel()
        {
            var timerPeriod = TimeSpan.FromMilliseconds(20);
            QueueChecker = new Timer(state => { CheckQueues(); }, null, timerPeriod, timerPeriod);
        }

        public static void Register(TimerElementHolder<T> element)
        {
            var tl = EnsureCurrentThreadHasQueue();
            try
            {
                tl.QueueLock.Get();
                tl.Queue.Enqueue(element);
            }
            finally
            {
                tl.QueueLock.Release();
            }
        }

        private static void CheckQueues()
        {
            foreach (var tl in AllThreadLocals.Current)
            {
                if (tl != null)
                {
                    try
                    {
                        if (!tl.QueueLock.TryGet())
                        {
                            continue;
                        }

                        CheckQueue(tl.Queue);
                    }
                    finally
                    {
                        tl.QueueLock.Release();
                    }
                }
            }
        }

        // Crawls through the callbacks and timeouts expired ones
        private static void CheckQueue(Queue<TimerElementHolder<T>> queue)
        {
            var now = DateTime.UtcNow;
            while (true)
            {
                if (queue.Count == 0)
                {
                    return;
                }

                var element = queue.Peek();
                if (element.IsCancellationRequested)
                {
                    queue.Dequeue();
                    continue;
                }

                if (element.DueTime < now)
                {
                    queue.Dequeue();

                    element.OnTimeout();
                }
            }
        }

        private static TimerWheelQueueThreadLocals<T> EnsureCurrentThreadHasQueue()
        {
            if (queueThreadLocals == null)
            {
                queueThreadLocals = new TimerWheelQueueThreadLocals<T>();
                AllThreadLocals.Add(queueThreadLocals);
            }

            return queueThreadLocals;
        }
    }

    internal abstract class TimerElementHolder<T> where T : IOnTimeout
    {
        protected TimerElementHolder(DateTime dueTime, T target)
        {
            this.DueTime = dueTime;
            this.Target = target;
        }

        /// <summary>
        /// The UTC time after which this timer should fire.
        /// </summary>
        public DateTime DueTime { get; }

        public T Target { get; }

        /// <summary>
        /// True if the timer is cancelled, false otherwise.
        /// </summary>
        public abstract bool IsCancellationRequested { get; }

        public void OnTimeout() => this.Target?.OnTimeout();
    }

    internal sealed class DelayTimerElementHolder : TimerElementHolder<DelayAwaitable>
    {
        public DelayTimerElementHolder(DateTime dueTime, DelayAwaitable target) : base(dueTime, target)
        {
        }

        public override bool IsCancellationRequested => false;
    }

    internal interface IOnTimeout
    {
        void OnTimeout();
    }
    
    internal sealed class TimerWheelQueueThreadLocals<T> where T : IOnTimeout
    {
        public readonly InterlockedExchangeLock QueueLock = new InterlockedExchangeLock();
        public readonly Queue<TimerElementHolder<T>> Queue = new Queue<TimerElementHolder<T>>();
    }

    /// <summary>Provides an awaitable context for switching into a target environment.</summary>
    /// <remarks>This type is intended for compiler use only.</remarks>
    public sealed class DelayAwaitable : ICriticalNotifyCompletion, IOnTimeout
    {
        private static readonly Action CompletedSentinel = () => { };
        private static readonly Action<object> TaskSchedulerCallback = RunAction;
        private static readonly WaitCallback WaitCallbackRunAction = RunAction;
        private static readonly SendOrPostCallback SendOrPostCallbackRunAction = RunAction;
        private static readonly ContextCallback ContextCallback = RunActionOnExecutionContext;

        private object scheduler;
        private ExecutionContext executionContext;
        private Action continuationAction;

        /// <summary>Gets an awaiter for this <see cref="DelayAwaitable"/>.</summary>
        /// <returns>An awaiter for this awaitable.</returns>
        /// <remarks>This method is intended for compiler user rather than use directly in code.</remarks>
        public DelayAwaitable GetAwaiter() => this;

        public bool IsCompleted => ReferenceEquals(this.continuationAction, CompletedSentinel);
        
        /// <inheritdoc />
        public void OnCompleted(Action continuation) => this.RegisterContinuation(continuation, true);

        /// <inheritdoc />
        public void UnsafeOnCompleted(Action continuation) => this.RegisterContinuation(continuation, flowContext: false);

        private void RegisterContinuation(Action continuation, bool flowContext)
        {
            if (flowContext)
            {
                this.executionContext = ExecutionContext.Capture();
            }

            this.scheduler = CaptureScheduler();

            var previousContinuation = Interlocked.CompareExchange(ref this.continuationAction, continuation, null);
            if (!ReferenceEquals(previousContinuation, null))
            {
                if (!ReferenceEquals(previousContinuation, CompletedSentinel))
                {
                    ThrowMultipleContinuationsException();
                }

                this.ScheduleContinuation();
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowMultipleContinuationsException() => throw new InvalidOperationException($"A single {nameof(DelayAwaitable)} can not have multiple continuations scheduled.");

        private static object CaptureScheduler()
        {
            var syncCtx = SynchronizationContext.Current;
            if (syncCtx != null && syncCtx.GetType() != typeof(SynchronizationContext))
            {
                return syncCtx;
            }

            var taskScheduler = TaskScheduler.Current;
            if (taskScheduler != TaskScheduler.Default)
            {
                return taskScheduler;
            }

            return null;
        }
        
        private void ScheduleContinuation()
        {
            if (this.scheduler is TaskScheduler taskScheduler)
            {
                Task.Factory.StartNew(TaskSchedulerCallback, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, taskScheduler);
            }
            else if (this.scheduler is SynchronizationContext syncCtx && syncCtx.GetType() != typeof(SynchronizationContext))
            {
                // Get the current SynchronizationContext, and if there is one,
                // post the continuation to it.  However, treat the base type
                // as if there wasn't a SynchronizationContext, since that's what it
                // logically represents.
                syncCtx.Post(SendOrPostCallbackRunAction, this);
            }
            else if (this.scheduler != null)
            {
                ThreadPool.QueueUserWorkItem(WaitCallbackRunAction, this);
            }
            else
            {
                ThreadPool.UnsafeQueueUserWorkItem(WaitCallbackRunAction, this);
            }
        }

        /// <summary>Runs an Action delegate provided as state.</summary>
        /// <param name="state">The Action delegate to invoke.</param>
        private static void RunAction(object state)
        {
            var self = (DelayAwaitable)state;
            var continuation = Interlocked.Exchange(ref self.continuationAction, CompletedSentinel);
            if (continuation != null)
            {
                var ec = self.executionContext;
                if (ec != null)
                {
                    ExecutionContext.Run(ec, ContextCallback, continuation);
                }
                else
                {
                    continuation();
                }
            }
        }

        private static void RunActionOnExecutionContext(object state)
        {
            var self = (Action)state;
            self?.Invoke();
        }

        /// <summary>Ends the await operation.</summary>
        public void GetResult()
        {
            if (this.IsCompleted) return;

            var spinWait = new SpinWait();
            while (!this.IsCompleted)
            {
                spinWait.SpinOnce();
            }
        }

        public void OnTimeout() => this.ScheduleContinuation();
    }

    internal class SparseArray<T> where T : class
    {
        private volatile T[] array;

        internal SparseArray(int initialSize)
        {
            this.array = new T[initialSize];
        }

        internal T[] Current => this.array;

        internal int Add(T e)
        {
            while (true)
            {
                var currentArray = this.array;
                lock (currentArray)
                {
                    for (int i = 0; i < currentArray.Length; i++)
                    {
                        if (currentArray[i] == null)
                        {
                            Volatile.Write(ref currentArray[i], e);
                            return i;
                        }
                        else if (i == currentArray.Length - 1)
                        {
                            // Must resize. If there was a race condition, we start over again.
                            if (currentArray != this.array)
                                continue;

                            T[] newArray = new T[currentArray.Length * 2];
                            Array.Copy(currentArray, newArray, i + 1);
                            newArray[i + 1] = e;
                            this.array = newArray;
                            return i + 1;
                        }
                    }
                }
            }
        }

        internal void Remove(T e)
        {
            while (true)
            {
                T[] currentArray = this.array;
                lock (currentArray)
                {
                    if (currentArray != this.array)
                    {
                        continue;
                    }

                    for (var i = 0; i < currentArray.Length; i++)
                    {
                        if (currentArray[i] == e)
                        {
                            Volatile.Write(ref currentArray[i], null);
                            break;
                        }
                    }

                    return;
                }
            }
        }
    }
}