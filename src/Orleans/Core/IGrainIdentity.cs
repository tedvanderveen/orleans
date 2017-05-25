using System;
using Orleans.Runtime;

namespace Orleans.Core
{
    public interface IGrainIdentity
    {
        Guid PrimaryKey { get; }

        long PrimaryKeyLong { get; }

        string PrimaryKeyString { get; }

        string IdentityString { get; }

        bool IsClient { get; }

        int TypeCode { get; }

        long GetPrimaryKeyLong(out string keyExt);

        Guid GetPrimaryKey(out string keyExt);

        uint GetUniformHashCode();
    }

public interface IGrainReferenceFactory
{
    GrainReference GetGrain(Type interfaceType, Guid primaryKey, string keyExtension, string grainClassNamePrefix);
    GrainReference GetGrain(Type interfaceType, long primaryKey, string keyExtension, string grainClassNamePrefix);
    GrainReference GetGrain(Type interfaceType, Guid primaryKey, string grainClassNamePrefix);
    GrainReference GetGrain(Type interfaceType, long primaryKey, string grainClassNamePrefix);
    GrainReference GetGrain(Type interfaceType, string primaryKey, string grainClassNamePrefix);
}
}
