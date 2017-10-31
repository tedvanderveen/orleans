using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;

namespace Orleans.ServiceFabric.Actors.Runtime
{
    internal sealed class ActorStateManager : IActorStateManager
    {
        private readonly Dictionary<string, StateMetadata> stateChangeTracker;
        private readonly ActorState state;
        private readonly Func<Task> writeStateAsync;
        
        public ActorStateManager(ActorState state, Func<Task> writeStateAsync)
        {
            this.state = state;
            this.writeStateAsync = writeStateAsync;
            this.stateChangeTracker = new Dictionary<string, StateMetadata>();
        }

        #region IActorStateManager Members

        public async Task AddStateAsync<T>(string stateName, T value, CancellationToken cancellationToken)
        {
            if (!(await this.TryAddStateAsync(stateName, value, cancellationToken)))
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, Actors.SR.ActorStateAlreadyExists, stateName));
            }
        }

        public Task<bool> TryAddStateAsync<T>(string stateName, T value, CancellationToken cancellationToken)
        {
            if (stateName == null) throw new ArgumentNullException(nameof(stateName));

            if (this.stateChangeTracker.ContainsKey(stateName))
            {
                var stateMetadata = this.stateChangeTracker[stateName];

                // Check if the property was marked as remove in the cache
                if (stateMetadata.ChangeKind == StateChangeKind.Remove)
                {
                    this.stateChangeTracker[stateName] = StateMetadata.Create(value, StateChangeKind.Update);
                    return Task.FromResult(true);
                }

                return Task.FromResult(false);
            }

            if (this.state.Values.TryGetValue(stateName, out var _))
            {
                return Task.FromResult(false);
            }

            this.stateChangeTracker[stateName] = StateMetadata.Create(value, StateChangeKind.Add);
            return Task.FromResult(true);
        }

        public async Task<T> GetStateAsync<T>(string stateName, CancellationToken cancellationToken)
        {
            var condRes = await this.TryGetStateAsync<T>(stateName, cancellationToken);

            if (condRes.HasValue)
            {
                return condRes.Value;
            }

            throw new KeyNotFoundException(string.Format(CultureInfo.CurrentCulture, SR.ErrorNamedActorStateNotFound, stateName));
        }

        public async Task<ConditionalValue<T>> TryGetStateAsync<T>(string stateName, CancellationToken cancellationToken)
        {
            if (stateName == null) throw new ArgumentNullException(nameof(stateName));

            if (this.stateChangeTracker.ContainsKey(stateName))
            {
                var stateMetadata = this.stateChangeTracker[stateName];

                // Check if the property was marked as remove in the cache
                if (stateMetadata.ChangeKind == StateChangeKind.Remove)
                {
                    return new ConditionalValue<T>(false, default(T));
                }

                return new ConditionalValue<T>(true, (T)stateMetadata.Value);
            }

            var conditionalResult = await this.TryGetStateFromStateProviderAsync<T>(stateName, cancellationToken);
            if (conditionalResult.HasValue)
            {
                this.stateChangeTracker.Add(stateName, StateMetadata.Create(conditionalResult.Value, StateChangeKind.None));
            }

            return conditionalResult;
        }

        public Task SetStateAsync<T>(string stateName, T value, CancellationToken cancellationToken)
        {
            if (stateName == null) throw new ArgumentNullException(nameof(stateName));

            if (this.stateChangeTracker.ContainsKey(stateName))
            {
                var stateMetadata = this.stateChangeTracker[stateName];
                stateMetadata.Value = value;

                if (stateMetadata.ChangeKind == StateChangeKind.None ||
                    stateMetadata.ChangeKind == StateChangeKind.Remove)
                {
                    stateMetadata.ChangeKind = StateChangeKind.Update;
                }
            }
            else if (this.state.Values.ContainsKey(stateName))
            {
                this.stateChangeTracker.Add(stateName, StateMetadata.Create(value, StateChangeKind.Update));
            }
            else
            {
                this.stateChangeTracker[stateName] = StateMetadata.Create(value, StateChangeKind.Add);
            }

            return Task.CompletedTask;
        }

        public async Task RemoveStateAsync(string stateName, CancellationToken cancellationToken)
        {
            if (!(await this.TryRemoveStateAsync(stateName, cancellationToken)))
            {
                throw new KeyNotFoundException(string.Format(CultureInfo.CurrentCulture, SR.ErrorNamedActorStateNotFound, stateName));
            }
        }

        public Task<bool> TryRemoveStateAsync(string stateName, CancellationToken cancellationToken)
        {
            if (stateName == null) throw new ArgumentNullException(nameof(stateName));

            if (this.stateChangeTracker.ContainsKey(stateName))
            {
                var stateMetadata = this.stateChangeTracker[stateName];

                switch (stateMetadata.ChangeKind)
                {
                    case StateChangeKind.Remove:
                        return Task.FromResult(false);
                    case StateChangeKind.Add:
                        this.stateChangeTracker.Remove(stateName);
                        return Task.FromResult(true);
                }

                stateMetadata.ChangeKind = StateChangeKind.Remove;
                return Task.FromResult(true);
            }

            if (this.state.Values.ContainsKey(stateName))
            {
                this.stateChangeTracker.Add(stateName, StateMetadata.CreateForRemove());
                return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }

        public Task<bool> ContainsStateAsync(string stateName, CancellationToken cancellationToken)
        {
            if (stateName == null) throw new ArgumentNullException(nameof(stateName));

            if (this.stateChangeTracker.ContainsKey(stateName))
            {
                var stateMetadata = this.stateChangeTracker[stateName];

                // Check if the property was marked as remove in the cache
                return Task.FromResult(stateMetadata.ChangeKind != StateChangeKind.Remove);
            }

            if (this.state.Values.ContainsKey(stateName))
            {
                return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }

        public async Task<T> GetOrAddStateAsync<T>(string stateName, T value, CancellationToken cancellationToken)
        {
            var condRes = await this.TryGetStateAsync<T>(stateName, cancellationToken);

            if (condRes.HasValue)
            {
                return condRes.Value;
            }

            var changeKind = this.IsStateMarkedForRemove(stateName) ? StateChangeKind.Update : StateChangeKind.Add;

            this.stateChangeTracker[stateName] = StateMetadata.Create(value, changeKind);
            return value;
        }

        public async Task<T> AddOrUpdateStateAsync<T>(
            string stateName,
            T addValue,
            Func<string, T, T> updateValueFactory,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (stateName == null) throw new ArgumentNullException(nameof(stateName));

            if (this.stateChangeTracker.ContainsKey(stateName))
            {
                var stateMetadata = this.stateChangeTracker[stateName];

                // Check if the property was marked as remove in the cache
                if (stateMetadata.ChangeKind == StateChangeKind.Remove)
                {
                    this.stateChangeTracker[stateName] = StateMetadata.Create(addValue, StateChangeKind.Update);
                    return addValue;
                }

                var newValue = updateValueFactory.Invoke(stateName, (T)stateMetadata.Value);
                stateMetadata.Value = newValue;

                if (stateMetadata.ChangeKind == StateChangeKind.None)
                {
                    stateMetadata.ChangeKind = StateChangeKind.Update;
                }

                return newValue;
            }

            var conditionalResult = await this.TryGetStateFromStateProviderAsync<T>(stateName, cancellationToken);
            if (conditionalResult.HasValue)
            {
                var newValue = updateValueFactory.Invoke(stateName, conditionalResult.Value);
                this.stateChangeTracker.Add(stateName, StateMetadata.Create(newValue, StateChangeKind.Update));

                return newValue;
            }

            this.stateChangeTracker[stateName] = StateMetadata.Create(addValue, StateChangeKind.Add);
            return addValue;
        }

        public Task<IEnumerable<string>> GetStateNamesAsync(CancellationToken cancellationToken = default(CancellationToken))        {
            var namesFromStateProvider = this.state.Values.Keys;
            var stateNameList = new List<string>(namesFromStateProvider);
            
            foreach (var change in this.stateChangeTracker)
            {
                switch (change.Value.ChangeKind)
                {
                    case StateChangeKind.Add:
                        stateNameList.Add(change.Key);
                        break;
                    case StateChangeKind.Remove:
                        stateNameList.Remove(change.Key);
                        break;
                }
            }

            return Task.FromResult<IEnumerable<string>>(stateNameList);
        }

        public Task ClearCacheAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            this.stateChangeTracker.Clear();
            return Task.CompletedTask;
        }

        public async Task SaveStateAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (this.stateChangeTracker.Count > 0)
            {
                var stateChangeList = new List<ActorStateChange>();
                var statesToRemove = new List<string>();

                foreach (var stateName in this.stateChangeTracker.Keys)
                {
                    var stateMetadata = this.stateChangeTracker[stateName];

                    if (stateMetadata.ChangeKind != StateChangeKind.None)
                    {
                        stateChangeList.Add(
                            new ActorStateChange(stateName, stateMetadata.Type, stateMetadata.Value, stateMetadata.ChangeKind));

                        if (stateMetadata.ChangeKind == StateChangeKind.Remove)
                        {
                            statesToRemove.Add(stateName);
                        }

                        stateMetadata.ChangeKind = StateChangeKind.None;
                    }
                }

                if (stateChangeList.Count > 0)
                {
                    await this.writeStateAsync();
                }

                foreach (var stateToRemove in statesToRemove)
                {
                    this.stateChangeTracker.Remove(stateToRemove);
                }
            }
        }

        #endregion

        private bool IsStateMarkedForRemove(string stateName)
        {
            if (this.stateChangeTracker.ContainsKey(stateName) &&
                this.stateChangeTracker[stateName].ChangeKind == StateChangeKind.Remove)
            {
                return true;
            }

            return false;
        }

        private Task<ConditionalValue<T>> TryGetStateFromStateProviderAsync<T>(string stateName, CancellationToken cancellationToken)
        {
            ConditionalValue<T> result;
            
            if (this.state.Values.TryGetValue(stateName, out var value))
            {
                result = new ConditionalValue<T>(true, (T)value);
            }
            else
            {
                result = new ConditionalValue<T>(false, default(T));
            }
            
            return Task.FromResult(result);
        }

        #region Helper Classes

        private sealed class StateMetadata
        {
            private readonly Type type;

            private StateMetadata(object value, Type type, StateChangeKind changeKind)
            {
                this.Value = value;
                this.type = type;
                this.ChangeKind = changeKind;
            }

            public object Value { get; set; }

            public StateChangeKind ChangeKind { get; set; }

            public Type Type
            {
                get { return this.type; }
            }

            public static StateMetadata Create<T>(T value, StateChangeKind changeKind)
            {
                return new StateMetadata(value, typeof(T), changeKind);
            }

            public static StateMetadata CreateForRemove()
            {
                return new StateMetadata(null, typeof(object), StateChangeKind.Remove);
            }
        }

        #endregion
    }
}