using System.Threading.Tasks;

namespace Orleans.MetadataStore
{
    // ReSharper disable once TypeParameterCanBeVariant
    public interface IAcceptor<TValue>
    {
        Task<PrepareResponse> Prepare(Ballot proposerConfigBallot, Ballot ballot);
        Task<AcceptResponse> Accept(Ballot proposerConfigBallot, Ballot ballot, TValue value);
    }
}