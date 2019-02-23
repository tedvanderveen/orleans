using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Runtime.GrainDirectory;

namespace Orleans.Runtime.Messaging
{
    internal sealed class MessageTrace
    {
        public void OnHandleMessage(Message message) { }

        internal void OnDropMessage(Message message, string reason) { }

        internal void OnRejectMessage(Message message, string reason) { }

        internal void OnInboundPing(Message message) { }
    }

    internal sealed class IdealMessageCenter : IMessageHandler
    {
        private readonly ILocalGrainDirectory localDirectory;
        private readonly ILogger<IdealMessageCenter> logger;
        private readonly SiloAddress localSiloAddress;
        private readonly MessageTrace trace;
        private readonly SiloMessagingOptions messagingOptions;
        private readonly MessageFactory messageFactory;
        private bool isBlockingApplicationMessages;

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
                this.RejectMessage(message, "No target silo provided.");
                return;
            }

            // If message is destined for remote silo:
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

                    return;
                default:
                    Debug.Assert(false, "Unknown message category: " + message.Category);
                    return;
            }

            // Switch on message.Category:
            //   * If Ping:
            //     * Send ping response
            //   * If System or Application:
            //     * If recipient is SystemTarget:
            //       * Find SystemTarget in Catalog
            //       * Enqueue message against system target
            //     * If recipient is Grain:
            //       * Find activation in catalog
            //       * If activation not found
            //       * If activation is not valid, reject with NonExistentActivationException
            //     * If recipient is Client:
            //       * If recipient is HostedClient, forward to hosted client
            //       * Otherwise, find client in gateway cache
            //       * Forward message to client
        }

        private void HandleInboundPingMessage(Message message)
        {
            this.trace.OnInboundPing(message);
            var response = this.messageFactory.CreateResponseMessage(message);
            response.BodyObject = Response.Done;
            this.SendMessageToRemoteSilo(response);
        }

        private void OnUnknownMessageCategory(Message message)
        {
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

            this.RejectMessage(message, reason);
        }

        private void DropMessage(Message message, string reason)
        {
            this.trace.OnDropMessage(message, reason);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void RejectMessage(Message message, string reason)
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
            //   * If remote silo is known to be dead (according to silo oracle), reject
            //   * Get handler for remote silo
            //   * If not found, reject
            //   * Forward to remote silo handler
        }
    }
}
