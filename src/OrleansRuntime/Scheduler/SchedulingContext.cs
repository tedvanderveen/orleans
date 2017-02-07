using System;

namespace Orleans.Runtime.Scheduler
{
    internal class SchedulingContext : ISchedulingContext
    {
        public SystemTarget SystemTarget { get; }

        public int DispatcherTarget { get; }

        public SchedulingContextType ContextType { get; }

        private readonly bool isLowPrioritySystemTarget;

        internal SchedulingContext(SystemTarget systemTarget, bool lowPrioritySystemTarget)
        {
            SystemTarget = systemTarget;
            ContextType = SchedulingContextType.SystemTarget;
            isLowPrioritySystemTarget = lowPrioritySystemTarget;
        }

        internal SchedulingContext(int dispatcherTarget)
        {
            DispatcherTarget = dispatcherTarget;
            ContextType = SchedulingContextType.SystemThread;
            isLowPrioritySystemTarget = false;
        }

        public bool IsSystemPriorityContext
        {
            get
            {
                switch (ContextType)
                {
                    case SchedulingContextType.SystemTarget:
                        return !isLowPrioritySystemTarget;

                    case SchedulingContextType.SystemThread:
                        return true;

                    default:
                        return false;
                }
            }
        }

        #region IEquatable<ISchedulingContext> Members

        public bool Equals(ISchedulingContext other)
        {
            return AreSame(other);
        }

        #endregion

        public override bool Equals(object obj)
        {
            return AreSame(obj);
        }

        private bool AreSame(object obj)
        {
            var other = obj as SchedulingContext;
            switch (ContextType)
            {
                case SchedulingContextType.SystemTarget:
                    return other != null && SystemTarget.Equals(other.SystemTarget);

                case SchedulingContextType.SystemThread:
                    return other != null && DispatcherTarget.Equals(other.DispatcherTarget);

                default:
                    return false;
            }
        }

        public override int GetHashCode()
        {
            switch (ContextType)
            {
                case SchedulingContextType.SystemTarget:
                    return SystemTarget.ActivationId.Key.GetHashCode();

                case SchedulingContextType.SystemThread:
                    return DispatcherTarget;

                default:
                    return 0;
            }
        }

        public override string ToString()
        {
            switch (ContextType)
            {
                case SchedulingContextType.SystemTarget:
                    return SystemTarget.ToString();

                case SchedulingContextType.SystemThread:
                    return String.Format("DispatcherTarget{0}", DispatcherTarget);

                default:
                    return ContextType.ToString();
            }
        }
        public string DetailedStatus()
        {
            switch (ContextType)
            {
                case SchedulingContextType.SystemTarget:
                    return SystemTarget.ToDetailedString();

                case SchedulingContextType.SystemThread:
                    return String.Format("DispatcherTarget{0}", DispatcherTarget);

                default:
                    return ContextType.ToString();
            }
        }

        public string Name 
        {
            get
            {
                switch (ContextType)
                {
                    case SchedulingContextType.SystemTarget:
                        return ((ISystemTargetBase)SystemTarget).GrainId.ToString();

                    case SchedulingContextType.SystemThread:
                        return String.Format("DispatcherTarget{0}", DispatcherTarget);

                    default:
                        return ContextType.ToString();
                }
            }
        }
    }
}
