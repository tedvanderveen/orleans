using System.Threading;
using System.Threading.Tasks;

namespace Orleans.MetadataStore
{
    public delegate TValue ChangeFunction<in TArg, TValue>(TValue existingValue, TArg newValue);

    public interface IProposer<TValue>
    {
        Task<(ReplicationStatus, TValue)> TryUpdate<TArg>(TArg value, ChangeFunction<TArg, TValue> changeFunction, CancellationToken cancellationToken);
    }
}