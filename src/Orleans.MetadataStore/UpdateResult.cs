using System;

namespace Orleans.MetadataStore
{
    [Serializable]
    public struct UpdateResult<TState>
    {
        public UpdateResult(bool success, TState value)
        {
            Success = success;
            Value = value;
        }

        public TState Value { get; set; }

        public bool Success { get; set; }

        public override string ToString()
        {
            return $"{nameof(UpdateResult<TState>)}({nameof(Success)}: {this.Success}, {nameof(Value)}: {this.Value})";
        }
    }
}