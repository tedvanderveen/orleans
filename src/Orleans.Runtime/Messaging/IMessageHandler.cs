namespace Orleans.Runtime.Messaging
{
    internal interface IMessageHandler
    {
        void HandleMessage(Message message);
    }
}
