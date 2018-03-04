using System;
using Orleans.Concurrency;

namespace Orleans.MetadataStore
{
    [Serializable]
    public abstract class PrepareResponse
    {
        public static PrepareSuccess<TValue> Success<TValue>(Ballot accepted, TValue value) => new PrepareSuccess<TValue>(accepted, value);
        public static PrepareConflict Conflict(Ballot conflicting) => new PrepareConflict(conflicting);
        public static PrepareConfigConflict ConfigConflict(Ballot conflicting) => new PrepareConfigConflict(conflicting);
    }

    [Serializable]
    public class PrepareSuccess<TValue> : PrepareResponse
    {
        public PrepareSuccess(Ballot accepted, TValue value)
        {
            Accepted = accepted;
            Value = value;
        }

        public TValue Value { get; }

        public Ballot Accepted { get; }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{nameof(PrepareSuccess<TValue>)}({nameof(Accepted)}: {Accepted}, {nameof(Value)}: {Value})";
        }
    }

    [Immutable]
    [Serializable]
    public class PrepareConflict : PrepareResponse
    {
        public PrepareConflict(Ballot conflicting)
        {
            this.Conflicting = conflicting;
        }

        public Ballot Conflicting { get; }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{nameof(PrepareConflict)}({nameof(Conflicting)}: {Conflicting})";
        }
    }

    [Immutable]
    [Serializable]
    public class PrepareConfigConflict : PrepareResponse
    {
        public PrepareConfigConflict(Ballot conflicting)
        {
            this.Conflicting = conflicting;
        }

        public Ballot Conflicting { get; }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{nameof(PrepareConfigConflict)}({nameof(Conflicting)}: {Conflicting})";
        }
    }

    [Serializable]
    public abstract class AcceptResponse
    {
        public static AcceptSuccess Success() => AcceptSuccess.Instance;

        public static AcceptConflict Conflict(Ballot conflicting) => new AcceptConflict(conflicting);

        public static AcceptConfigConflict ConfigConflict(Ballot conflicting) => new AcceptConfigConflict(conflicting);
    }

    [Immutable]
    [Serializable]
    public class AcceptSuccess : AcceptResponse
    {
        public static AcceptSuccess Instance { get; } = new AcceptSuccess();

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{nameof(AcceptSuccess)}()";
        }
    }

    [Immutable]
    [Serializable]
    public class AcceptConflict : AcceptResponse
    {
        public AcceptConflict(Ballot conflicting)
        {
            Conflicting = conflicting;
        }

        public Ballot Conflicting { get; }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{nameof(AcceptConflict)}({nameof(Conflicting)}: {Conflicting})";
        }
    }

    [Immutable]
    [Serializable]
    public class AcceptConfigConflict : AcceptResponse
    {
        public AcceptConfigConflict(Ballot conflicting)
        {
            Conflicting = conflicting;
        }

        public Ballot Conflicting { get; }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{nameof(AcceptConflict)}({nameof(Conflicting)}: {Conflicting})";
        }
    }
}
