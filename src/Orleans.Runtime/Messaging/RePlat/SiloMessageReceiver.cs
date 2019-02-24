using System;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Logging;

namespace Orleans.Runtime.Messaging
{
    internal sealed class SiloMessageReceiver : ConnectionMessageReceiver
    {
        private readonly IdealMessageCenter messageCenter;
        private readonly ILogger<SiloMessageReceiver> log;
        private readonly MessageFactory messageFactory;

        public SiloMessageReceiver(ConnectionContext connection, IMessageSerializer serializer, IdealMessageCenter messageCenter, MessageFactory messageFactory, ILogger<SiloMessageReceiver> log) : base(connection, serializer)
        {
            this.messageCenter = messageCenter;
            this.messageFactory = messageFactory;
            this.log = log;
        }


        protected override void OnReceivedMessage(Message msg)
        {
            this.messageCenter.HandleMessage(msg);
        }
    }
}
