using System.Reflection;
using Orleans.EventSourcing.StateStorage;
using Orleans.Runtime.Configuration;
using Orleans.Serialization;
using Tester.Serialization;
using TestExtensions;
using Xunit;

namespace Tester.EventSourcingTests
{
    [CollectionDefinition(EventSourceEnvironmentFixture.EventSource)]
    public class TestEnvironmentFixtureCollection : ICollectionFixture<EventSourceEnvironmentFixture> { }

    public class EventSourceEnvironmentFixture : SerializationTestEnvironment
    {
        // Force load of OrleansEventSourcing
        private static readonly GrainStateWithMetaDataAndETag<object> dummy = new GrainStateWithMetaDataAndETag<object>();

        public const string EventSource = "EventSourceTestEnvironment";

        public EventSourceEnvironmentFixture() : base(MixinDefaults(
            new ClientConfiguration
            {
                // Replace the default fallback serializer with a serializer which cannot serialize any of the
                // EventSourcing types.
                FallbackSerializationProvider = typeof(FakeSerializer).GetTypeInfo()
            }))
        {
        }

        public T RoundTripSerialization<T>(T source)
        {
            BinaryTokenStreamWriter writer = new BinaryTokenStreamWriter();
            SerializationManager.Serialize(source, writer);
            T output = (T)SerializationManager.Deserialize(new BinaryTokenStreamReader(writer.ToByteArray()));

            return output;
        }
    }
}