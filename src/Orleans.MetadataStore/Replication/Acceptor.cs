using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.MetadataStore.Storage;
using AsyncEx = Nito.AsyncEx;

namespace Orleans.MetadataStore
{
    public class Acceptor<TValue> : IAcceptor<TValue>
    {
        private readonly AsyncEx.AsyncLock lockObj;
        private readonly ILocalStore store;
        private readonly Func<Ballot> getConfigurationBallot;
        private readonly Action<RegisterState<TValue>> onUpdateState;
        private readonly string key;
        private readonly ILogger log;
        private RegisterState<TValue> state;

        public Acceptor(
            string key,
            ILocalStore store,
            Func<Ballot> getConfigurationBallot,
            Action<RegisterState<TValue>> onUpdateState,
            ILogger log)
        {
            this.lockObj = new AsyncEx.AsyncLock();
            this.key = key;
            this.store = store;
            this.getConfigurationBallot = getConfigurationBallot;
            this.onUpdateState = onUpdateState;
            this.log = log;
        }
        
        public async Task<PrepareResponse> Prepare(Ballot proposerConfigBallot, Ballot ballot)
        {
            using (await this.lockObj.LockAsync())
            {
                // Initialize the register state if it's not yet initialized.
                await EnsureStateLoadedNoLock();

                PrepareResponse result;
                var configBallot = this.getConfigurationBallot();
                if (configBallot > proposerConfigBallot)
                {
                    // If the proposer is using a cluster configuration version which is lower than the highest
                    // cluster configuration version observed by this node, then the Prepare is rejected.
                    result = PrepareResponse.ConfigConflict(configBallot);
                }
                else
                {
                    if (this.state.Promised > ballot)
                    {
                        // If a Prepare with a higher ballot has already been encountered, reject this.
                        result = PrepareResponse.Conflict(this.state.Promised);
                    }
                    else if (this.state.Accepted > ballot)
                    {
                        // If an Accept with a higher ballot has already been encountered, reject this.
                        result = PrepareResponse.Conflict(this.state.Accepted);
                    }
                    else
                    {
                        // Record a tentative promise to accept this proposer's value.
                        var newState = new RegisterState<TValue>(ballot, this.state.Accepted, this.state.Value);
                        await this.store.Write(this.key, newState);
                        this.state = newState;

                        result = PrepareResponse.Success(this.state.Accepted, this.state.Value);
                    }
                }

                LogPrepare(proposerConfigBallot, ballot, result);
                return result;
            }
        }

        public async Task<AcceptResponse> Accept(Ballot proposerConfigBallot, Ballot ballot, TValue value)
        {
            using (await this.lockObj.LockAsync())
            {
                // Initialize the register state if it's not yet initialized.
                await this.EnsureStateLoadedNoLock();

                AcceptResponse result;
                var configBallot = this.getConfigurationBallot();
                if (configBallot > proposerConfigBallot)
                {
                    // If the proposer is using a cluster configuration version which is lower than the highest
                    // cluster configuration version observed by this node, then the Accept is rejected.
                    result = AcceptResponse.ConfigConflict(configBallot);
                }
                else
                {
                    if (this.state.Promised > ballot)
                    {
                        // If a Prepare with a higher ballot has already been encountered, reject this.
                        result = AcceptResponse.Conflict(this.state.Promised);
                    }
                    else if (this.state.Accepted > ballot)
                    {
                        // If an Accept with a higher ballot has already been encountered, reject this.
                        result = AcceptResponse.Conflict(this.state.Accepted);
                    }
                    else
                    {
                        // Record the new state.
                        var newState = new RegisterState<TValue>(ballot, ballot, value);
                        await this.store.Write(this.key, newState);
                        this.state = newState;
                        this.onUpdateState?.Invoke(this.state);
                        result = AcceptResponse.Success();
                    }
                }

                LogAccept(ballot, value, result);
                return result;
            }
        }

        internal RegisterState<TValue> State => this.state;

        internal async Task ForceState(TValue newState)
        {
            using (await this.lockObj.LockAsync())
            {
                this.state = new RegisterState<TValue>(Ballot.Zero, Ballot.Zero, newState);
                this.onUpdateState?.Invoke(this.state);
            }
        }

        private void LogPrepare(Ballot configVersion, Ballot ballot, PrepareResponse result)
        {
            if (this.log.IsEnabled(LogLevel.Information)) this.log.LogInformation($"Prepare(config: {configVersion}, ballot: {ballot}) -> {result}");
        }

        private void LogAccept(Ballot ballot, TValue value, AcceptResponse result)
        {
            if (this.log.IsEnabled(LogLevel.Information)) this.log.LogInformation($"Accept({ballot}, {value}) -> {result}");
        }

        internal async Task EnsureStateLoaded()
        {
            using (await this.lockObj.LockAsync())
            {
                await EnsureStateLoadedNoLock();
            }
        }

        private async Task EnsureStateLoadedNoLock()
        {
            if (this.state == null)
            {
                var stored = await this.store.Read<RegisterState<TValue>>(this.key);
                this.state = stored ?? RegisterState<TValue>.Default;
                this.onUpdateState?.Invoke(this.state);

                this.log.LogInformation($"Initialized with register state {this.state}");
            }
        }
    }
}