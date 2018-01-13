using System;
using System.Threading.Tasks;


namespace Orleans.Runtime.Scheduler
{
    internal interface IWorkItem
    {
        string Name { get; }
        WorkItemType ItemType { get; }
        ISchedulingContext SchedulingContext { get; set; }
        TimeSpan TimeSinceQueued { get; }
        DateTime TimeQueued { get; set;  }
        bool IsSystemPriority { get; }
        TaskScheduler Scheduler { get; set; }
        void Execute();
    }
}
