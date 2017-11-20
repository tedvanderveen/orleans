namespace Orleans.Serialization
{
    /// <summary>
    /// Values for identifying <see cref="IKeyedSerializer"/> serializers.
    /// </summary>
    internal enum KeyedSerializerId : byte
    {
        /// <summary>
        /// <see cref="Orleans.Serialization.ILBasedSerializer"/>
        /// </summary>
        ILBasedSerializer = 1,

        /// <summary>
        /// <see cref="Orleans.Serialization.DotNetSerializableSerializer"/>
        /// </summary>
        DotNetSerializableSerializer = 2,

        /// <summary>
        /// The maximum reserved value.
        /// </summary>
        MaxReservedValue = 100,
    }
}