using System;
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
            var result = new DelayAwaitable(dueTime);
            TimerWheel<DelayAwaitable>.Register(result);
            return result;
        }
    }

    internal static class TimerWheel<T> where T : class, ITimerCallback
    {
        private static volatile TimerWheelQueueThreadLocals[] allThreadLocalsArray = new TimerWheelQueueThreadLocals[16];

        [ThreadStatic]
        private static TimerWheelQueueThreadLocals queueThreadLocals;

        private static TimerWheelQueueThreadLocals[] AllThreadLocals => allThreadLocalsArray;

        // ReSharper disable once StaticMemberInGenericType
        private static readonly Timer QueueChecker;

        static TimerWheel()
        {
            var timerPeriod = TimeSpan.FromMilliseconds(20);
            QueueChecker = new Timer(state => { CheckQueues(); }, null, timerPeriod, timerPeriod);
        }

        public static void Register(T element)
        {
            var tl = EnsureCurrentThreadHasQueue();
            try
            {
                tl.QueueLock.Get();

                // If this is the first element, update the head.
                if (tl.Head is null) tl.Head = element;

                // If this is not the first element, update the current tail.
                var prevTail = tl.Tail;
                if (!(prevTail is null)) prevTail.Next = element;

                // Update the tail.
                tl.Tail = element;
            }
            finally
            {
                tl.QueueLock.Release();
            }
        }

        private static void CheckQueues()
        {
            foreach (var tl in AllThreadLocals)
            {
                var acquired = false;
                if (tl != null)
                {
                    try
                    {
                        if (!tl.QueueLock.TryGet())
                        {
                            continue;
                        }

                        acquired = true;
                        CheckQueueInLock(tl);
                    }
                    finally
                    {
                        if (acquired) tl.QueueLock.Release();
                    }
                }
            }
        }

        // Crawls through the callbacks and fires expired ones
        private static void CheckQueueInLock(TimerWheelQueueThreadLocals queue)
        {
            var now = DateTime.UtcNow;
            var prev = default(T);
            for (var current = queue.Head; current != null; prev = current, current = current.Next as T)
            {
                if (current.IsCanceled)
                {
                    Dequeue(queue, prev, current);

                    continue;
                }

                if (current.DueTime < now)
                {
                    Dequeue(queue, prev, current);

                    current.OnTimeout();
                }
            }

            void Dequeue(TimerWheelQueueThreadLocals locals, T previous, T current)
            {
                var next = current.Next as T;

                if (!(previous is null)) previous.Next = next;

                if (ReferenceEquals(locals.Head, current))
                {
                    locals.Head = next;
                }

                if (ReferenceEquals(locals.Tail, current))
                {
                    locals.Tail = previous;
                }
            }
        }

        private static TimerWheelQueueThreadLocals EnsureCurrentThreadHasQueue()
        {
            if (queueThreadLocals == null)
            {
                queueThreadLocals = new TimerWheelQueueThreadLocals();
                AddThreadLocals(queueThreadLocals);

                void AddThreadLocals(TimerWheelQueueThreadLocals threadLocals)
                {
                    while (true)
                    {
                        var currentArray = allThreadLocalsArray;
                        lock (currentArray)
                        {
                            if (currentArray != allThreadLocalsArray)
                            {
                                continue;
                            }

                            for (var i = 0; i < currentArray.Length; i++)
                            {
                                if (currentArray[i] == null)
                                {
                                    Volatile.Write(ref currentArray[i], threadLocals);
                                    return;
                                }

                                if (i == currentArray.Length - 1)
                                {
                                    var newArray = new TimerWheelQueueThreadLocals[currentArray.Length * 2];
                                    Array.Copy(currentArray, newArray, i + 1);
                                    newArray[i + 1] = threadLocals;
                                    allThreadLocalsArray = newArray;
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            return queueThreadLocals;
        }

        private sealed class TimerWheelQueueThreadLocals
        {
            public readonly RecursiveInterlockedExchangeLock QueueLock = new RecursiveInterlockedExchangeLock();
            public T Head;
            public T Tail;
        }
    }

    internal interface ITimerCallback
    {
        DateTime DueTime { get; }

        bool IsCanceled { get; }

        void OnTimeout();

        /// <summary>
        /// The next timer. This value must never be accessed or modified by user code.
        /// </summary>
        ITimerCallback Next { get; set; }
    }

    /// <summary>
    /// Provides an awaitable which completes at a specified time.
    /// </summary>
    public sealed class DelayAwaitable : ICriticalNotifyCompletion, ITimerCallback
    {
        private static readonly Action CompletedSentinel = () => { };
        private static readonly Action<object> TaskSchedulerCallback = RunAction;
        private static readonly WaitCallback WaitCallbackRunAction = RunAction;
        private static readonly SendOrPostCallback SendOrPostCallbackRunAction = RunAction;
        private static readonly ContextCallback ContextCallback = RunActionOnExecutionContext;

        private object scheduler;
        private ExecutionContext executionContext;
        private Action continuationAction;

        internal DelayAwaitable(DateTime expiry)
        {
            this.DueTime = expiry;
        }

        public DateTime DueTime { get; }

        public bool IsCanceled => false;

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

            // Register the continuation
            var previousContinuation = Interlocked.CompareExchange(ref this.continuationAction, continuation, null);

            // Check that this is the only registered continuation.
            if (!ReferenceEquals(previousContinuation, null))
            {
                if (ReferenceEquals(previousContinuation, CompletedSentinel))
                {
                    // This awaitable has already completed, schedule the provided continuation.
                    this.ScheduleContinuation();
                }
                else
                {
                    // This awaitable only supports a single continuation, therefore throw if the user
                    // tries to register multiple.
                    ThrowMultipleContinuationsException();
                }
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

        ITimerCallback ITimerCallback.Next { get; set; }
    }
}