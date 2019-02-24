using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime.GrainDirectory;
using Orleans.Runtime.Scheduler;

namespace Orleans.Runtime.Messaging
{
    internal sealed class MessageTrace
    {
        public void OnHandleMessage(Message message) { }

        internal void OnDropMessage(Message message, string reason) { }

        internal void OnRejectMessage(Message message, string reason) { }

        internal void OnInboundPing(Message message) { }
    }

    internal sealed class IdealMessageCenter : IMessageHandler, ILifecycleParticipant<ISiloLifecycle>
    {
        private OrleansTaskScheduler scheduler;
        private ILocalGrainDirectory localDirectory;
        private ActivationDirectory activationDirectory;
        private ILogger<IdealMessageCenter> logger;
        private SiloAddress localSiloAddress;
        private MessageTrace trace;
        private SiloMessagingOptions messagingOptions;
        private MessageFactory messageFactory;
        private Dispatcher dispatcher;
        private Gateway gateway;
        private ConnectionManager connectionManager;
        private ISiloStatusOracle siloStatusOracle;
        private IHostedClient hostedClient;
        private IServiceProvider serviceProvider;
        private bool isBlockingApplicationMessages;

        public IdealMessageCenter(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider;
        }

        /// <summary>
        /// Handles all incoming and outgoing messages.
        /// </summary>
        /// <param name="message"></param>
        public void HandleMessage(Message message)
        {
            Debug.Assert(message != null);
            this.trace.OnHandleMessage(message);

            // If expired, drop.
            if (message.IsExpired)
            {
                this.DropMessage(message, "Expired");
                return;
            }

            // If destination silo is not specified, reject
            if (message.TargetSilo == null)
            {
                this.SendRejectionMessage(message, "No target silo provided.");
                return;
            }

            // If message is destined for remote silo, send it onwards.
            if (!message.TargetSilo.Matches(this.localSiloAddress))
            {
                if (message.SendingSilo.Matches(this.localSiloAddress))
                {
                    // Send to remote silo.
                    this.SendMessageToRemoteSilo(message);
                    return;
                }
                else
                {
                    // Forward to remote silo.
                    this.ForwardMessageToRemoteSilo(message);
                    return;
                }
            }

            // Otherwise, message is destined for local silo or a client attached to the local silo.

            // If destined for an older generation of this silo, send rejection.
            if (message.TargetSilo.Generation != 0 && message.TargetSilo.Generation < this.localSiloAddress.Generation)
            {
                this.RejectMessageToOlderGeneration(message);
                return;
            }

            // If messaging has CacheInvalidationHeaders, invalidate those entries.
            var invalidActivations = message.CacheInvalidationHeader;
            if (invalidActivations != null)
            {
                this.InvalidateActivations(message, invalidActivations);
            }

            // Block application messages during shutdown.
            if (this.isBlockingApplicationMessages
                && message.Category == Message.Categories.Application
                && !Constants.SystemMembershipTableId.Equals(message.SendingGrain))
            {
                this.BlockApplicationMessage(message);
                return;
            }

            switch (message.Category)
            {
                case Message.Categories.Ping:
                    this.HandleInboundPingMessage(message);
                    return;
                case Message.Categories.Application:
                case Message.Categories.System:
                    this.HandleInboundMessage(message);
                    return;
                default:
                    Debug.Assert(false, "Unknown message category: " + message.Category);
                    return;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void HandleInboundPingMessage(Message message)
        {
            this.trace.OnInboundPing(message);
            var response = this.messageFactory.CreateResponseMessage(message);
            response.BodyObject = Response.Done;
            this.SendMessageToRemoteSilo(response);
        }

        private void HandleInboundMessage(Message message)
        {
            switch (message.TargetGrain.Category)
            {
                case UniqueKey.Category.Client:
                case UniqueKey.Category.GeoClient:
                    this.HandleInboundMessageToClient(message);
                    return;
                case UniqueKey.Category.Grain:
                case UniqueKey.Category.KeyExtGrain:
                    this.HandleInboundMessageToGrain(message);
                    return;
                case UniqueKey.Category.SystemTarget:
                    this.HandleInboundMessageToSystemTarget(message);
                    return;
                case UniqueKey.Category.SystemGrain:
                default:
                    this.HandleInboundMessageToUnknownTarget(message);
                    return;
            }
        }

        private void HandleInboundMessageToUnknownTarget(Message message)
        {
            throw new NotImplementedException();
        }

        private void HandleInboundMessageToSystemTarget(Message message)
        {
            var target = this.activationDirectory.FindSystemTarget(message.TargetActivation);
            if (target == null)
            {
                this.RejectMessageToUnknownSystemTarget(message);
                return;
            }

            var context = target.SchedulingContext;
            switch (message.Direction)
            {
                case Message.Directions.Request:
                    MessagingProcessingStatisticsGroup.OnImaMessageEnqueued(context);
                    scheduler.QueueWorkItem(new RequestWorkItem(target, message), context);
                    break;

                case Message.Directions.Response:
                    MessagingProcessingStatisticsGroup.OnImaMessageEnqueued(context);
                    scheduler.QueueWorkItem(new ResponseWorkItem(target, message), context);
                    break;

                default:
                    this.logger.Error(ErrorCode.Runtime_Error_100097, "Invalid message: " + message);
                    break;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void RejectMessageToUnknownSystemTarget(Message message)
        {
            MessagingStatisticsGroup.OnRejectedMessage(message);
            Message response = this.messageFactory.CreateRejectionResponse(message, Message.RejectionTypes.Unrecoverable,
                string.Format("SystemTarget {0} not active on this silo. Msg={1}", message.TargetGrain, message));
            this.HandleMessage(response);
            this.logger.Warn(ErrorCode.MessagingMessageFromUnknownActivation, "Received a message {0} for an unknown SystemTarget: {1}", message, message.TargetAddress);
        }

        private void HandleInboundMessageToClient(Message message)
        {
            if (!this.gateway.TryDeliverToProxy(message) && hostedClient == null || !hostedClient.TryDispatchToClient(message))
            {
                this.DropMessageToUnknownClient(message);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void DropMessageToUnknownClient(Message message)
        {
            this.trace.OnDropMessage(message, $"Client {message.TargetGrain} is unknown.");
        }

        private void HandleInboundMessageToGrain(Message message)
        {
            // Run this code on the target activation's context, if it already exists
            var target = this.activationDirectory.FindTarget(message.TargetActivation);
            if (target != null)
            {
                ISchedulingContext context;
                if (target.State == ActivationState.Valid)
                {
                    // Response messages are not subject to overload checks.
                    if (message.Direction != Message.Directions.Response)
                    {
                        var overloadException = target.CheckOverloaded(this.logger);
                        if (overloadException != null)
                        {
                            // Send rejection as soon as we can, to avoid creating additional work for runtime
                            this.RejectMessageToOverloadedActivation(message, target, overloadException);
                            return;
                        }
                    }

                    // Run ReceiveMessage in context of target activation
                    context = target.SchedulingContext;
                }
                else
                {
                    // Can't use this activation - will queue for another activation
                    target = null;
                    context = null;
                }

                EnqueueReceiveMessage(message, target, context);
            }
            else
            {
                // No usable target activation currently, so run ReceiveMessage in system context
                EnqueueReceiveMessage(message, null, null);
            }

            void EnqueueReceiveMessage(Message msg, ActivationData activation, ISchedulingContext ctx)
            {
                MessagingProcessingStatisticsGroup.OnImaMessageEnqueued(ctx);

                if (activation != null) activation.IncrementEnqueuedOnDispatcherCount();

                scheduler.QueueWorkItem(new ClosureWorkItem(() =>
                {
                    try
                    {
                        this.dispatcher.ReceiveMessage(msg);
                    }
                    finally
                    {
                        if (activation != null) activation.DecrementEnqueuedOnDispatcherCount();
                    }
                },
                "ReceiveMessage"), ctx);
            }
        }

        private void RejectMessageToOverloadedActivation(Message message, ActivationData target, LimitExceededException overloadException)
        {
            var reason = "Target activation is overloaded " + target;
            var rejection = this.messageFactory.CreateRejectionResponse(message, Message.RejectionTypes.Overloaded, reason, overloadException);
            this.SendRejectionMessage(rejection, reason);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void BlockApplicationMessage(Message message)
        {
            // Reject requests, drop all other messages.
            if (message.Direction == Message.Directions.Request)
            {
                var rejection = this.messageFactory.CreateRejectionResponse(message, Message.RejectionTypes.Unrecoverable, "Silo stopping");
                this.HandleMessage(rejection);
            }
            else
            {
                this.DropMessage(message, "Blocking application messages");
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void InvalidateActivations(Message message, List<ActivationAddress> invalidActivations)
        {
            foreach (var address in invalidActivations)
            {
                this.localDirectory.InvalidateCacheEntry(address, message.IsReturnedFromRemoteCluster);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void RejectMessageToOlderGeneration(Message message)
        {
            MessagingStatisticsGroup.OnRejectedMessage(message);
            var reason = string.Format(
                "The target silo is no longer active: target was {0}, but this silo is {1}. The rejected message is {2}.",
                message.TargetSilo.ToLongString(),
                this.localSiloAddress.ToLongString(),
                message);
            var rejection = this.messageFactory.CreateRejectionResponse(message, Message.RejectionTypes.Transient, reason);

            // Invalidate the remote caller's activation cache entry.
            if (message.TargetAddress != null) rejection.AddToCacheInvalidationHeader(message.TargetAddress);

            this.SendRejectionMessage(message, reason);
        }

        private void DropMessage(Message message, string reason)
        {
            this.trace.OnDropMessage(message, reason);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void SendRejectionMessage(Message message, string reason)
        {
            Debug.Assert(message.Result == Message.ResponseTypes.Rejection);
            this.trace.OnRejectMessage(message, reason);

            // Reenter top-level handler.
            // The rejection might be for a local caller or a client connected to the local gateway.
            this.HandleMessage(message);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ForwardMessageToRemoteSilo(Message message)
        {
            // If forward count > max forward count:
            //   * If ResponseType is not Rejection, reject
            //   * Otherwise, drop
            if (message.ForwardCount > this.messagingOptions.MaxForwardCount)
            {
                this.DropMessage(message, "Exceeded maximum allowed forward count.");
                return;
            }

            message.ForwardCount++;
            this.SendMessageToRemoteSilo(message);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void SendMessageToRemoteSilo(Message message)
        {
            // If remote silo is known to be dead (according to silo oracle), reject
            var targetSilo = message.TargetSilo;
            if (this.siloStatusOracle.IsDeadSilo(targetSilo))
            {
                this.RejectMessageToKnownDeadSilo(message);
                return;
            }

            var sender = this.connectionManager.GetConnection(targetSilo.Endpoint.ToString());
            sender.Send(message);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void RejectMessageToKnownDeadSilo(Message message)
        {
            var reason = string.Format("Target {0} silo is known to be dead", message.TargetSilo.ToLongString());
            var rejection = this.messageFactory.CreateRejectionResponse(message, Message.RejectionTypes.Transient, reason);
            this.SendRejectionMessage(rejection, reason);
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(
                "MessageCenter",
                ServiceLifecycleStage.RuntimeInitialize,
                OnRuntimeInitializeStart,
                OnRuntimeInitializeStop);

            Task OnRuntimeInitializeStart(CancellationToken cancellationToken)
            {
                this.localSiloAddress = this.serviceProvider.GetRequiredService<ILocalSiloDetails>().SiloAddress;
                this.hostedClient = this.serviceProvider.GetRequiredService<HostedClient>();
                this.scheduler = this.serviceProvider.GetRequiredService<OrleansTaskScheduler>();
                this.localDirectory = this.serviceProvider.GetRequiredService<ILocalGrainDirectory>();
                this.activationDirectory = this.serviceProvider.GetRequiredService<ActivationDirectory>();
                this.logger = this.serviceProvider.GetRequiredService<ILogger<IdealMessageCenter>>();
                this.trace = this.serviceProvider.GetRequiredService<MessageTrace>();
                this.messageFactory = this.serviceProvider.GetRequiredService<MessageFactory>();
                this.dispatcher = this.serviceProvider.GetRequiredService<Dispatcher>();
                this.gateway = this.serviceProvider.GetRequiredService<Gateway>();
                this.connectionManager = this.serviceProvider.GetRequiredService<ConnectionManager>();
                this.siloStatusOracle = this.serviceProvider.GetRequiredService<ISiloStatusOracle>();
                this.messagingOptions = this.serviceProvider.GetRequiredService<IOptions<SiloMessagingOptions>>().Value;
                this.isBlockingApplicationMessages = false;
                return Task.CompletedTask;
            }

            Task OnRuntimeInitializeStop(CancellationToken cancellationToken)
            {
                this.isBlockingApplicationMessages = true;
                return Task.CompletedTask;
            }
        }
    }
}
