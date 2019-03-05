using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;

namespace Orleans.Runtime.Messaging
{
    internal abstract class ConnectionMessageReceiver
    {
        private readonly IMessageSerializer serializer;
        private readonly CancellationTokenSource cancellation = new CancellationTokenSource();

        protected ConnectionMessageReceiver(ConnectionContext connection, IMessageSerializer serializer)
        {
            this.Connection = connection;
            this.serializer = serializer;
        }

        protected ConnectionContext Connection { get; }

        public void Abort()
        {
            ThreadPool.UnsafeQueueUserWorkItem(cts => ((CancellationTokenSource)cts).Cancel(), this.cancellation);
        }

        public Task Run() => Task.Run(this.Process);

        protected abstract void OnReceivedMessage(Message message);

        protected abstract void OnReceiveMessageFail(Message message, Exception exception);

        private async Task Process()
        {
            var input = this.Connection.Transport.Input;
            var error = default(Exception);
            try
            {
                var requiredBytes = 0;
                Message message = default;
                while (!this.cancellation.IsCancellationRequested)
                {
                    var readResultTask = input.ReadAsync(this.cancellation.Token);
                    var readResult = readResultTask.IsCompletedSuccessfully ? readResultTask.GetAwaiter().GetResult() : await readResultTask;
                    
                    var buffer = readResult.Buffer;
                    
                    if (buffer.Length >= requiredBytes)
                    {
                        do
                        {
                            try
                            {
                                requiredBytes = this.serializer.TryRead(ref buffer, out message);
                                if (requiredBytes == 0)
                                {
                                    this.OnReceivedMessage(message);
                                }
                            }
                            catch (Exception readException)
                            {
                                this.OnReceiveMessageFail(message, readException);
                                throw;
                            }
                        } while (requiredBytes == 0);
                    }

                    if (readResult.IsCanceled || readResult.IsCompleted) break;
                    input.AdvanceTo(buffer.Start, buffer.End);
                }
            }
            catch (Exception exception)
            {
                error = exception;
            }
            finally
            {
                this.Abort();

                if (error != null)
                {
                    this.Connection.Abort(new ConnectionAbortedException($"Exception in {nameof(ConnectionMessageReceiver)}, see {nameof(Exception.InnerException)}.", error));
                }
                else
                {
                    this.Connection.Abort();
                }
            }
        }
    }
}
