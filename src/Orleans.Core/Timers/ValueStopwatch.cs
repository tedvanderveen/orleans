using System;
using System.Diagnostics;

namespace Orleans.Runtime
{
    internal struct ValueStopwatch
    {
        private static readonly double TimestampToTicks = TimeSpan.TicksPerSecond / (double) Stopwatch.Frequency;
        private long value;

        public static ValueStopwatch StartNew() => new ValueStopwatch(Stopwatch.GetTimestamp());

        private ValueStopwatch(long timestamp)
        {
            this.value = timestamp;
        }

        public TimeSpan GetElapsedTime()
        {
            // A positive timestamp value indicates the start time of a running stopwatch,
            // a negative value indicates the negative total duration of a stopped stopwatch.
            var timestamp = this.value;

            // Timestamp can't be zero in an initialized instance. It would have to be literally the first thing executed when the machine boots to be 0.
            // So it being 0 is a clear indication of default(ValueStopwatch)
            if (timestamp == 0) ThrowInvalidInstance();

            long delta;
            if (timestamp > 0)
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

            return new TimeSpan((long) (delta * TimestampToTicks));
        }

        public long GetRawTimestamp() => this.value;

        public void Stop()
        {
            var timestamp = this.value;
            if (timestamp > 0)
            {
                var end = Stopwatch.GetTimestamp();
                var delta = end - timestamp;

                // A value of 0 is considered invalid, so shunt 0 values.
                if (delta == 0) delta = 1;

                this.value = -delta;
            }
        }

        private static void ThrowInvalidInstance()
        {
            throw new InvalidOperationException($"An uninitialized, or 'default', {nameof(ValueStopwatch)} cannot be used to get elapsed time.");
        }
    }
}