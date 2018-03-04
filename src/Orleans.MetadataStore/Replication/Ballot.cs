using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Orleans.Concurrency;

namespace Orleans.MetadataStore
{
    [Serializable]
    [Immutable]
    public struct Ballot : IComparable<Ballot>, IComparable, ISerializable
    {
        /// <summary>
        /// The proposal number.
        /// </summary>
        [JsonProperty]
        public readonly int Counter;

        /// <summary>
        /// The unique identifier of the proposer.
        /// </summary>
        [JsonProperty]
        public readonly int Id;

        public Ballot(int counter, int id)
        {
            this.Counter = counter;
            this.Id = id;
        }

        public Ballot(SerializationInfo info, StreamingContext context)
        {
            this.Counter = info.GetInt32(nameof(Counter));
            this.Id = info.GetInt32(nameof(Id));
        }

        public Ballot Successor() => new Ballot(this.Counter + 1, this.Id);

        public Ballot AdvanceTo(Ballot other) => new Ballot(Math.Max(this.Counter, other.Counter), this.Id);

        public static Ballot Zero => default(Ballot);

        public bool IsZero() => this == Zero;

        /// <inheritdoc />
        public override string ToString() => this.IsZero() ? $"{nameof(Ballot)}(ø)" : $"{nameof(Ballot)}({Counter}.{Id})";

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(nameof(Counter), this.Counter);
            info.AddValue(nameof(Id), this.Id);
        }

        public bool Equals(Ballot other)
        {
            return Counter == other.Counter && Id == other.Id;
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is Ballot ballot && Equals(ballot);
        }

        /// <inheritdoc />
        public int CompareTo(Ballot other)
        {
            var counterComparison = Counter - other.Counter;
            if (counterComparison != 0) return counterComparison;
            return Id - other.Id;
        }

        /// <inheritdoc />
        public int CompareTo(object obj)
        {
            if (ReferenceEquals(null, obj)) return 1;
            if (!(obj is Ballot)) throw new ArgumentException($"Object must be of type {nameof(Ballot)}");
            return CompareTo((Ballot) obj);
        }

        public static bool operator ==(Ballot left, Ballot right) => left.Equals(right);

        public static bool operator !=(Ballot left, Ballot right) => !left.Equals(right);

        public static bool operator <(Ballot left, Ballot right)
        {
            return left.CompareTo(right) < 0;
        }

        public static bool operator >(Ballot left, Ballot right)
        {
            return left.CompareTo(right) > 0;
        }

        public static bool operator <=(Ballot left, Ballot right)
        {
            return left.CompareTo(right) <= 0;
        }

        public static bool operator >=(Ballot left, Ballot right)
        {
            return left.CompareTo(right) >= 0;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Counter * 397) ^ Id;
            }
        }
    }
}