using System.Threading;

namespace Orleans.Threading
{
    internal class InterlockedExchangeLock
    {
        private const int Unlocked = -1;
        private int lockState = Unlocked;

        public bool TryGet()
        {
            var threadId = Thread.CurrentThread.ManagedThreadId;
            var previousValue = Interlocked.CompareExchange(ref this.lockState, threadId, Unlocked);
            return previousValue == threadId || previousValue == Unlocked;
        }

        public void Get()
        {
            if (this.TryGet())
                return;

            var spinWait = new SpinWait();
            while (!this.TryGet())
            {
                spinWait.SpinOnce();
            }
        }

        public void Release() => Interlocked.Exchange(ref this.lockState, Unlocked);
    }
}
