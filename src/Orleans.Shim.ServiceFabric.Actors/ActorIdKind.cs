namespace Orleans.ServiceFabric.Actors.Runtime
{
    /// <summary>
    /// Specifies the type of the ID value for an <see cref="ActorId"/>.
    /// </summary>
    public enum ActorIdKind
    {
        /// <summary>
        /// Represents ID value of type <see cref="System.Int64"/>.
        /// </summary>
        Long = 0,

        /// <summary>
        /// Represents ID value of type <see cref="System.Guid"/>.
        /// </summary>
        Guid = 1,

        /// <summary>
        /// Represents ID value of type <see cref="System.String"/>.
        /// </summary>
        String = 2
    }
}