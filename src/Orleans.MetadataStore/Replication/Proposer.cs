using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using AsyncEx = Nito.AsyncEx;

namespace Orleans.MetadataStore
{
    public class Proposer<TValue> : IProposer<TValue>
    {
        private readonly AsyncEx.AsyncLock lockObj;
        private readonly ILogger log;
        private readonly string key;
        private readonly Func<ExpandedReplicaSetConfiguration> getConfiguration;
        private bool skipPrepare;
        private int nextInstanceId;
        private TValue cachedValue;

        public Proposer(string key, Ballot initialBallot, Func<ExpandedReplicaSetConfiguration> getConfiguration, ILogger log)
        {
            this.lockObj = new AsyncEx.AsyncLock();
            this.key = key;
            this.Ballot = initialBallot;
            this.getConfiguration = getConfiguration;
            this.log = log;
        }

        internal Ballot Ballot { get; set; }

        public async Task<(ReplicationStatus, TValue)> TryUpdate<TArg>(TArg value, ChangeFunction<TArg, TValue> changeFunction, CancellationToken cancellationToken)
        {
            using (await this.lockObj.LockAsync(cancellationToken))
            {
                return await TryUpdateInternal(value, changeFunction, cancellationToken, numRetries: 1);
            }
        }

        private async Task<(ReplicationStatus, TValue)> TryUpdateInternal<TArg>(
            TArg value,
            ChangeFunction<TArg, TValue> changeFunction,
            CancellationToken cancellationToken,
            int numRetries)
        {
            // Configuration is observed once per attempt.
            // If this node's configuration changes while this proposer is still attempting to commit a value, the commit will
            // continue under the old configuration. If that configuration has already been observed by some of the acceptors,
            // the commit may fail and in that case the proposer may retry.
            var config = this.getConfiguration();

            // Select a ballot number for this attempt. The ballot must be consistent between propose and accept for the attempt.
            var prepareBallot = this.Ballot = this.Ballot.Successor();

            TValue currentValue;
            if (this.skipPrepare)
            {
                // If this node is leader, attempt to skip the prepare phase and at go straight to another accept
                // phase, assuming that the value has not changed since this proposer last had a value accepted.
                currentValue = this.cachedValue;

                if (this.log.IsEnabled(LogLevel.Debug)) this.log.LogDebug($"Will attempt Accept using cached value, {currentValue}");
            }
            else
            {
                // Try to obtain a quorum of promises from the acceptors and simultaneously learn the currently accepted value.
                bool prepareSuccess;
                (prepareSuccess, currentValue) = await this.TryPrepare(prepareBallot, config, cancellationToken);
                this.cachedValue = currentValue;
                if (!prepareSuccess)
                {
                    // Allow the proposer to retry in order to hide harmless fast-forward events.
                    if (numRetries > 0)
                    {
                        if (this.log.IsEnabled(LogLevel.Information)) this.log.LogInformation("Prepare failed, will retry.");
                        return await this.TryUpdateInternal(value, changeFunction, cancellationToken, numRetries - 1);
                    }

                    if (this.log.IsEnabled(LogLevel.Information)) this.log.LogInformation("Prepare failed, no remaining retries.");
                    return (ReplicationStatus.Failed, currentValue);
                }

                if (this.log.IsEnabled(LogLevel.Information)) this.log.LogInformation($"Prepare succeeded, learned current value: {currentValue}");
            }

            // Modify the currently accepted value and attempt to have it accepted on all acceptors.
            var newValue = changeFunction(currentValue, value);
            if (this.log.IsEnabled(LogLevel.Information)) this.log.LogInformation($"Trying to have new value {newValue} accepted.");
            var acceptSuccess = await this.TryAccept(prepareBallot, newValue, config, cancellationToken);
            if (acceptSuccess)
            {
                // The accept succeeded, this proposer can attempt to use the current accept as a promise for a subsequent accept as an optimization.
                if (this.log.IsEnabled(LogLevel.Information)) this.log.LogInformation($"Successfully updated value to {newValue}.");
                this.skipPrepare = true;
                this.cachedValue = newValue;
                return (ReplicationStatus.Success, newValue);
            }

            // Since the accept did not succeed, this proposer issue a prepare before trying to have a value accepted.
            this.skipPrepare = false;

            if (numRetries > 0)
            {
                // This attempt may have failed because another proposer interfered, so attempt again to have this value accepted.
                if (this.log.IsEnabled(LogLevel.Information)) this.log.LogInformation("Accept failed, will retry. No longer assuming leadership.");
                return await this.TryUpdateInternal(value, changeFunction, cancellationToken, numRetries - 1);
            }

            // It is possible that the value was committed successfully without this node receiving a quorum of acknowledgements,
            // so the result is uncertain.
            // For example, an acceptor's acknowledgement message may have been lost in transmission due to a transient network fault.
            if (this.log.IsEnabled(LogLevel.Information)) this.log.LogInformation("Accept failed, no remaining retries.");
            return (ReplicationStatus.Uncertain, currentValue);
        }

        private async Task<(bool, TValue)> TryPrepare(Ballot prepareBallot, ExpandedReplicaSetConfiguration config, CancellationToken cancellationToken)
        {
            var prepareTasks = new List<Task<PrepareResponse>>(config.StoreReferences.Length);
            foreach (var acceptors in config.StoreReferences)
            {
                var acceptor = SelectInstance(acceptors);
                prepareTasks.Add(acceptor.Prepare(this.key, config.Configuration.Stamp, prepareBallot));
            }

            // Run a Prepare round in order to learn the current value of the register and secure a promise that a quorum
            // of nodes which accept our new value.
            var requiredConfirmations = config.Configuration.PrepareQuorum;
            var currentValue = default(TValue);
            var maxSuccess = Ballot.Zero;
            var maxConflict = Ballot.Zero;
            while (prepareTasks.Count > 0 && requiredConfirmations > 0 && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var resultTask = await Task.WhenAny(prepareTasks);
                    prepareTasks.Remove(resultTask);
                    var prepareResult = await resultTask;
                    switch (prepareResult)
                    {
                        case PrepareSuccess<TValue> success:
                            --requiredConfirmations;
                            if (success.Accepted >= maxSuccess)
                            {
                                maxSuccess = success.Accepted;
                                currentValue = success.Value;
                            }

                            break;
                        case PrepareConflict conflict:
                            if (conflict.Conflicting > maxConflict) maxConflict = conflict.Conflicting;
                            break;
                        case PrepareConfigConflict _:
                            // Nothing needs to be done when encountering a configuration conflict, however it
                            // poses a good opportunity to ensure that this node's configuration is up-to-date.
                            // TODO: Signal to configuration manager that we need to update configuration.
                            break;
                    }

                    // TODO: break the loop as soon as we receive a quorum of negative confirmations (and therefore cannot receive a quorum of positive confirmations).
                }
                catch (Exception exception)
                {
                    if (this.log.IsEnabled(LogLevel.Warning)) this.log.LogWarning($"Exception during Prepare: {exception}");
                }
            }

            // Advance the ballot to the highest conflicting ballot to improve the likelihood of the next attempt succeeding.
            if (maxConflict > this.Ballot) this.Ballot = this.Ballot.AdvanceTo(maxConflict);

            cancellationToken.ThrowIfCancellationRequested();

            var achievedQuorum = requiredConfirmations == 0;
            return (achievedQuorum, currentValue);
        }

        private IRemoteMetadataStore SelectInstance(IRemoteMetadataStore[] acceptors)
        {
            if (nextInstanceId >= acceptors.Length) nextInstanceId = 0;
            return acceptors[nextInstanceId++];
        }

        public delegate Task<AcceptResponse> Accept(IRemoteMetadataStore acceptor, string key, Ballot configStamp, Ballot ballot, TValue value);

        private async Task<bool> TryAccept(Ballot thisBallot, TValue newValue, ExpandedReplicaSetConfiguration config, CancellationToken cancellationToken)
        {
            // The prepare phase succeeded, proceed to propagate the new value to all acceptors.
            var acceptTasks = new List<Task<AcceptResponse>>(config.StoreReferences.Length);
            foreach (var acceptors in config.StoreReferences)
            {
                var acceptor = SelectInstance(acceptors);
                acceptTasks.Add(acceptor.Accept(this.key, config.Configuration.Stamp, thisBallot, newValue));
            }

            var requiredConfirmations = config.Configuration.AcceptQuorum;
            var maxConflict = Ballot.Zero;
            while (acceptTasks.Count > 0 && requiredConfirmations > 0 && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var resultTask = await Task.WhenAny(acceptTasks);
                    acceptTasks.Remove(resultTask);
                    var acceptResult = await resultTask;
                    switch (acceptResult)
                    {
                        case AcceptSuccess _:
                            --requiredConfirmations;
                            break;
                        case AcceptConflict conflict:
                            if (conflict.Conflicting > maxConflict) maxConflict = conflict.Conflicting;
                            break;
                        case AcceptConfigConflict _:
                            // Nothing needs to be done when encountering a configuration conflict, however it
                            // poses a good opportunity to ensure that this node's configuration is up-to-date.
                            // TODO: Signal to configuration manager that we need to update configuration?
                            break;
                    }

                    // TODO: break the loop as soon as we receive a quorum of negative confirmations (and therefore cannot receive a quorum of positive confirmations).
                }
                catch (Exception exception)
                {
                    if (this.log.IsEnabled(LogLevel.Warning)) this.log.LogWarning($"Exception during Accept: {exception}");
                }
            }

            // Advance the ballot past the highest conflicting ballot to improve the likelihood of the next Prepare succeeding.
            if (maxConflict > thisBallot) this.Ballot = this.Ballot.AdvanceTo(maxConflict);

            cancellationToken.ThrowIfCancellationRequested();

            var achievedQuorum = requiredConfirmations == 0;
            return achievedQuorum;
        }
    }
}