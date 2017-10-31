using System;
using System.Fabric;
using System.Runtime.Serialization;

namespace Orleans.ServiceFabric.Actors.Runtime
{
    [Serializable]
    internal sealed class ReminderLoadInProgressException : FabricTransientException
    {
        public ReminderLoadInProgressException()
        {
        }

        public ReminderLoadInProgressException(string message) : base(message)
        {
        }

        public ReminderLoadInProgressException(string message, Exception inner)
            : base(message, inner)
        {
        }

        private ReminderLoadInProgressException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}