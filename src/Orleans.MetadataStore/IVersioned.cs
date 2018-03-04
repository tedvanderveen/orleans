namespace Orleans.MetadataStore
{
    /// <summary>
    /// Represents an object which has a version number which increases by one with every revision.
    /// </summary>
    public interface IVersioned { long Version { get; } }
}