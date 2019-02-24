using Microsoft.Extensions.Logging;

namespace Orleans.Runtime.Messaging
{
    internal sealed class MessageTrace
    {
        private readonly ILogger logger;

        public MessageTrace(ILoggerFactory loggerFactory)
        {
            this.logger = loggerFactory.CreateLogger("Orleans.Messaging");
        }

        public void OnHandleMessage(Message message)
        {
            if (this.logger.IsEnabled(LogLevel.Information)) this.logger.LogInformation("Handling Message {Message}", message);
        }

        internal void OnDropMessage(Message message, string reason)
        {
            if (this.logger.IsEnabled(LogLevel.Information)) this.logger.LogInformation("Dropping Message {Message}. Reason: {Reason}", message, reason);
        }

        internal void OnRejectMessage(Message message, string reason)
        {
            if (this.logger.IsEnabled(LogLevel.Information)) this.logger.LogInformation("Rejecting Message {Message}. Reason: {Reason}", message, reason);
        }

        internal void OnInboundPing(Message message)
        {
            if (this.logger.IsEnabled(LogLevel.Information)) this.logger.LogInformation("Received ping message {Message}", message);
        }
    }
}
