using System;

namespace Orleans.MetadataStore
{
    /// <summary>
    /// The state of a register. This must be made locally durable before it is valid.
    /// </summary>
    /// <typeparam name="TValue">The type of the value stored in the register.</typeparam>
    [Serializable]
    public class RegisterState<TValue>
    {
        public RegisterState(Ballot promised, Ballot accepted, TValue value)
        {
            Promised = promised;
            Accepted = accepted;
            Value = value;
        }

        public static RegisterState<TValue> Default { get; } = new RegisterState<TValue>(Ballot.Zero, Ballot.Zero, default(TValue));
                
        /// <summary>
        /// The ballot that this node promised to commit to if no higher ballots come along before an accept request is made.
        /// </summary>
        public Ballot Promised { get; }

        /// <summary>
        /// The ballot corresponding to the most recently accepted value, held in <see cref="Value"/>.
        /// </summary>
        public Ballot Accepted { get; }

        /// <summary>
        /// The accepted value stored in this register.
        /// </summary>
        /// <remarks>
        /// Note that this value is not necessarily committed and will not necessarily ever be committed.
        /// In order to determine a valid value for this register, a consensus round must be performed.
        /// </remarks>
        public TValue Value { get; }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{nameof(RegisterState<TValue>)}({nameof(Promised)}: {Promised}, {nameof(Accepted)}: {Accepted}, {nameof(Value)}: {Value?.ToString() ?? "null"})";
        }
    }
}