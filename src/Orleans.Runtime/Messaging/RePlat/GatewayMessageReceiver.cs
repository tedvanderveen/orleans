using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Runtime.Messaging
{
    internal sealed class GatewayMessageReceiver : ConnectionMessageReceiver
    {
        private readonly IdealMessageCenter messageCenter;
        private readonly ILocalSiloDetails siloDetails;
        private readonly MultiClusterOptions multiClusterOptions;
        private readonly CounterStatistic loadSheddingCounter;
        private readonly CounterStatistic gatewayTrafficCounter;
        private readonly OverloadDetector overloadDetector;
        private readonly MessageFactory messageFactory;
        private readonly ILogger<GatewayMessageReceiver> log;
        private readonly Gateway gateway;

        public GatewayMessageReceiver(
            ConnectionContext connection,
            IMessageSerializer serializer,
            IdealMessageCenter messageCenter,
            ILocalSiloDetails siloDetails,
            OverloadDetector overloadDetector,
            IOptions<MultiClusterOptions> multiClusterOptions,
            MessageFactory messageFactory,
            ILogger<GatewayMessageReceiver> log,
            Gateway gateway)
            : base(connection, serializer)
        {
            this.messageCenter = messageCenter;
            this.siloDetails = siloDetails;
            this.overloadDetector = overloadDetector;
            this.messageFactory = messageFactory;
            this.log = log;
            this.gateway = gateway;
            this.multiClusterOptions = multiClusterOptions.Value;
            this.loadSheddingCounter = CounterStatistic.FindOrCreate(StatisticNames.GATEWAY_LOAD_SHEDDING);
            this.gatewayTrafficCounter = CounterStatistic.FindOrCreate(StatisticNames.GATEWAY_RECEIVED);
        }

        protected override void OnReceivedMessage(Message msg)
        {
            // Don't process messages that have already timed out
            if (msg.IsExpired)
            {
                msg.DropExpiredMessage(MessagingStatisticsGroup.Phase.Receive);
                return;
            }

            gatewayTrafficCounter.Increment();

            // return address translation for geo clients (replace sending address cli/* with gcl/*)
            if (this.multiClusterOptions.HasMultiClusterNetwork && msg.SendingAddress.Grain.Category != UniqueKey.Category.GeoClient)
            {
                msg.SendingGrain = GrainId.NewClientId(msg.SendingAddress.Grain.PrimaryKey, this.siloDetails.ClusterId);
            }

            // Are we overloaded?
            if (this.overloadDetector.Overloaded)
            {
                MessagingStatisticsGroup.OnRejectedMessage(msg);
                Message rejection = this.messageFactory.CreateRejectionResponse(msg, Message.RejectionTypes.GatewayTooBusy, "Shedding load");
                this.messageCenter.HandleMessage(rejection);
                if (this.log.IsEnabled(LogLevel.Debug)) this.log.Debug("Rejecting a request due to overloading: {0}", msg.ToString());
                loadSheddingCounter.Increment();
                return;
            }

            msg.SendingSilo = this.siloDetails.SiloAddress;
            msg.TargetSilo = null;

            this.messageCenter.HandleMessage(msg);
        }
    }
}
