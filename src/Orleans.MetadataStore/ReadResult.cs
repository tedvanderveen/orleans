using System;

namespace Orleans.MetadataStore
{
    [Serializable]
    public struct ReadResult<TValue> where TValue : class, IVersioned
    {
        public ReadResult(bool success, TValue value)
        {
            this.Success = success;
            this.Value = value;
        }

        public TValue Value { get; set; }

        public bool Success { get; set; }

        public override string ToString()
        {
            return $"{nameof(ReadResult<TValue>)}({nameof(Success)}: {this.Success}, {nameof(Value)}: {this.Value})";
        }
    }
}