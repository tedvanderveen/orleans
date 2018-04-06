using System;

namespace Orleans.Runtime
{
    [Serializable]
    internal struct Response
    {
        public bool ExceptionFlag => this.Exception != null;
        public Exception Exception { get; }
        public object Data { get; }

        public Response(object data)
        {
            Exception = data as Exception;
            Data = Exception == null ? data : null;
        }

        public static Response ExceptionResponse(Exception exc)
        {
            return new Response(exc);
        }

        public override string ToString()
        {
            return String.Format("Response ExceptionFlag={0}", ExceptionFlag);
        }

        public static Response Done { get; } = new Response(null);
    }
}
