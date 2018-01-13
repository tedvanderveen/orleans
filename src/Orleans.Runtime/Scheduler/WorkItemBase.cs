using System;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler
{
    internal abstract class WorkItemBase : ThisTask.ThisTask, IWorkItem
    {
        private static readonly Action<object> Action = obj => ((IWorkItem)obj).Execute();

        internal protected WorkItemBase() : base(Action)
        {
        }

        public ISchedulingContext SchedulingContext { get; set; }
        public TimeSpan TimeSinceQueued 
        {
            get { return Utils.Since(TimeQueued); } 
        }

        public abstract string Name { get; }

        public abstract WorkItemType ItemType { get; }

        public DateTime TimeQueued { get; set; }

        public abstract void Execute();

        public bool IsSystemPriority
        {
            get { return SchedulingUtils.IsSystemPriorityContext(this.SchedulingContext); }
        }

        public TaskScheduler Scheduler { get; set; }

        public override string ToString()
        {
            return string.Format("[{0} WorkItem Name={1}, Ctx={2}]", 
                ItemType, 
                Name ?? string.Empty,
                SchedulingContext?.ToString() ?? "null"
            );
        }
    }
}

