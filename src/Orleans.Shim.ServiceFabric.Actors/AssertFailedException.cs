using System;
using System.Runtime.Serialization;

namespace Orleans.ServiceFabric.Actors.Runtime
{
    [Serializable]
    public class AssertFailedException : Exception {
        public AssertFailedException()
        {
        }

        public AssertFailedException(string message) : base(message)
        {
        }

        public AssertFailedException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected AssertFailedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}