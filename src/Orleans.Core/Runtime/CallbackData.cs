using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Transactions;

namespace Orleans.Runtime
{
    internal class CallbackData
    {
        private readonly SharedCallbackData shared;
        private readonly TaskCompletionSource<object> context;
        private ValueStopwatch stopwatch;
        private bool alreadyFired;

        public CallbackData(
            SharedCallbackData shared,
            TaskCompletionSource<object> ctx, 
            Message msg)
        {
            this.shared = shared;
            this.context = ctx;
            this.Message = msg;
            this.TransactionInfo = TransactionContext.GetTransactionInfo();
            this.stopwatch = ValueStopwatch.StartNew();
        }

        public ITransactionInfo TransactionInfo { get; set; }

        public Message Message { get; set; } // might hold metadata used by response pipeline

        public bool IsCompleted => this.alreadyFired;

        public bool IsExpired(long currentTimestamp)
        {
            return currentTimestamp - this.stopwatch.GetRawTimestamp() > this.shared.ResponseTimeoutStopwatchTicks;
        }

        public void OnTimeout(TimeSpan timeout)
        {
            if (alreadyFired)
                return;
            var msg = Message; // Local working copy

            string messageHistory = msg.GetTargetHistory();
            string errorMsg = $"Response did not arrive on time in {timeout} for message: {msg}. Target History is: {messageHistory}.";
            this.shared.Logger.Warn(ErrorCode.Runtime_Error_100157, "{0} About to break its promise.", errorMsg);

            var error = Message.CreatePromptExceptionResponse(msg, new TimeoutException(errorMsg));
            OnFail(msg, error, "OnTimeout - Resend {0} for {1}", true);
        }

        public void OnTargetSiloFail()
        {
            if (alreadyFired)
                return;

            var msg = Message;
            var messageHistory = msg.GetTargetHistory();
            string errorMsg = 
                $"The target silo became unavailable for message: {msg}. Target History is: {messageHistory}. See {Constants.TroubleshootingHelpLink} for troubleshooting help.";
            this.shared.Logger.Warn(ErrorCode.Runtime_Error_100157, "{0} About to break its promise.", errorMsg);

            var error = Message.CreatePromptExceptionResponse(msg, new SiloUnavailableException(errorMsg));
            OnFail(msg, error, "On silo fail - Resend {0} for {1}");
        }

        public void DoCallback(Message response)
        {
            if (alreadyFired)
                return;
            lock (this)
            {
                if (alreadyFired)
                    return;

                if (response.Result == Message.ResponseTypes.Rejection && response.RejectionType == Message.RejectionTypes.Transient)
                {
                    if (this.shared.ShouldResend(Message))
                    {
                        return;
                    }
                }

                alreadyFired = true;
                if (StatisticsCollector.CollectApplicationRequestsStats)
                {
                    this.stopwatch.Stop();
                }
                this.shared.Unregister(Message);
            }
            if (StatisticsCollector.CollectApplicationRequestsStats)
            {
                ApplicationRequestsStatisticsGroup.OnAppRequestsEnd(this.stopwatch.GetElapsedTime());
            }

            // do callback outside the CallbackData lock. Just not a good practice to hold a lock for this unrelated operation.
            this.shared.ResponseCallback(response, context);
        }

        private void OnFail(Message msg, Message error, string resendLogMessageFormat, bool isOnTimeout = false)
        {
            lock (this)
            {
                if (alreadyFired)
                    return;

                if (this.shared.MessagingOptions.ResendOnTimeout && this.shared.ShouldResend(msg))
                {
                    if (this.shared.Logger.IsEnabled(LogLevel.Debug)) this.shared.Logger.Debug(resendLogMessageFormat, msg.ResendCount, msg);
                    return;
                }

                alreadyFired = true;
                if (StatisticsCollector.CollectApplicationRequestsStats)
                {
                    this.stopwatch.Stop();
                }

                this.shared.Unregister(Message);
            }
            
            if (StatisticsCollector.CollectApplicationRequestsStats)
            {
                ApplicationRequestsStatisticsGroup.OnAppRequestsEnd(this.stopwatch.GetElapsedTime());
                if (isOnTimeout)
                {
                    ApplicationRequestsStatisticsGroup.OnAppRequestsTimedOut();
                }
            }

            this.shared.ResponseCallback(error, context);
        }
    }
}
