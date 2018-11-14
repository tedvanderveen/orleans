using System;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Threading;
using Orleans;
using BenchmarkGrainInterfaces.Ping;

namespace BenchmarkGrains.Ping
{
    public class LoadGrain : Grain, ILoadGrain
    {
        private TaskCompletionSource<Report> runTask;

        public Task Generate(int runNumber, int total, int concurrency)
        {
            long succeeded = 0;
            long failed = 0;
            var capturedTotal = total;
            this.runTask = new TaskCompletionSource<Report>();
            var pipeline = new AsyncPipeline2(
                concurrency,
                () =>
                {
                    var num = Interlocked.Decrement(ref capturedTotal);
                    if (num >= 0)
                    {
                        // Do some work.
                        var grain = this.GrainFactory.GetGrain<IPingGrain>(total * runNumber + num);
                        return (grain.Run(), true);
                    }

                    // No more work.
                    return (null, false);
                },
                task =>
                {
                    // Tally the results.
                    switch (task.Status)
                    {
                        case TaskStatus.RanToCompletion:
                            Interlocked.Increment(ref succeeded);
                            break;

                        case TaskStatus.Canceled:
                        case TaskStatus.Faulted:
                            Interlocked.Increment(ref failed);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(task.Status), task.Status.ToString());
                    }
                });

            // Run the pipeline, timing the result.
            var stopwatch = Stopwatch.StartNew();
            pipeline.Run().ContinueWith(antecedent =>
            {
                if (antecedent.Exception != null)
                {
                    this.runTask.TrySetException(antecedent.Exception);
                    return;
                }

                stopwatch.Stop();

                this.runTask.TrySetResult(new Report
                {
                    Elapsed = stopwatch.Elapsed,
                    Failed = failed,
                    Succeeded = succeeded
                });
            }).Ignore();

            return Task.CompletedTask;
        }

        public async Task<Report> TryGetReport()
        {
            var task = this.runTask.Task;
            if (task.IsCompleted)
            {
                return await task;
            }

            return null;
        }
    }

    public class AsyncPipeline2
    {
        private readonly int concurrency;
        private int running;
        private readonly TaskCompletionSource<int> completed = new TaskCompletionSource<int>();
        private readonly Func<(Task, bool)> createTask;
        private readonly Action<Task> onTaskCompleted;
        private readonly Action<Task> onTaskCompletedHook;
        private readonly object lockObject = new object();

        public AsyncPipeline2(int concurrency, Func<(Task, bool)> createTask, Action<Task> onTaskCompletedHook)
        {
            if (concurrency < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(concurrency), "The pipeline size must be larger than 0.");
            }

            this.concurrency = concurrency;
            this.createTask = createTask ?? throw new ArgumentNullException(nameof(createTask));
            this.onTaskCompletedHook = onTaskCompletedHook;
            this.onTaskCompleted = this.OnTaskCompletion;
        }

        internal Task Run()
        {
            this.ScheduleWork();
            return this.completed.Task;
        }

        internal void ScheduleWork()
        {
            var numCreated = 0;
            var setCompleted = false;
            lock (this.lockObject)
            {
                while (this.running + numCreated <= this.concurrency)
                {
                    var (task, more) = this.createTask();
                    if (task != null)
                    {
                        ++numCreated;
                        task.ContinueWith(this.onTaskCompleted).Ignore();
                    }

                    if (task == null || !more)
                    {
                        break;
                    }
                }

                this.running += numCreated;
                if (this.running == 0 && numCreated == 0)
                {
                    setCompleted = true;
                }
            }

            if (setCompleted) this.completed.TrySetResult(0);
        }

        private void OnTaskCompletion(Task task)
        {
            this.onTaskCompletedHook?.Invoke(task);

            lock (this.lockObject) --this.running;

            this.ScheduleWork();
        }
    }
}
