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
            return Interlocked.CompareExchange(ref this.lockState, threadId, Unlocked) == threadId;
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
