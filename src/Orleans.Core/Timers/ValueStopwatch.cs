using System;
using System.Diagnostics;

namespace Orleans.Runtime
{
    internal struct ValueStopwatch
    {
        private static readonly double TimestampToTicks = TimeSpan.TicksPerSecond / (double) Stopwatch.Frequency;
        private long value;

        public static ValueStopwatch StartNew() => new ValueStopwatch(Stopwatch.GetTimestamp());

        public static ValueStopwatch Zero => default(ValueStopwatch);

        private ValueStopwatch(long timestamp)
        {
            this.value = timestamp;
        }

        public bool IsRunning => this.value > 0;
        
        public TimeSpan Elapsed => TimeSpan.FromTicks(this.ElapsedTicks);

        public long ElapsedTicks
        {
            get
            {
                // A positive timestamp value indicates the start time of a running stopwatch,
                // a negative value indicates the negative total duration of a stopped stopwatch.
                var timestamp = this.value;
                
                long delta;
                if (this.IsRunning)
                {
                    // The stopwatch is still running.
                    var start = timestamp;
                    var end = Stopwatch.GetTimestamp();
                    delta = end - start;
                }
                else
                {
                    // The stopwatch has been stopped.
                    delta = -timestamp;
                }

                return (long) (delta * TimestampToTicks);
            }
        }

        public long GetRawTimestamp() => this.value;

        public void Start()
        {
            var timestamp = this.value;
            
            // If already started, do nothing.
            if (this.IsRunning) return;

            // Stopwatch is stopped, therefore value is zero or negative.
            // Add the negative value to the current timestamp to start the stopwatch again.
            var newValue = Stopwatch.GetTimestamp() + timestamp;
            if (newValue == 0) newValue = 1;
            this.value = newValue;
        }

        public void Restart() => this.value = Stopwatch.GetTimestamp();

        public void Stop()
        {
            var timestamp = this.value;

            // If already stopped, do nothing.
            if (!this.IsRunning) return;

            var end = Stopwatch.GetTimestamp();
            var delta = end - timestamp;

            this.value = -delta;
        }
    }
}