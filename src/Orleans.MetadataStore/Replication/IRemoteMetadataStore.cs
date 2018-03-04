using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.MetadataStore
{
    public interface IRemoteMetadataStore : ISystemTarget
    {
        Task<PrepareResponse> Prepare(string key, Ballot proposerConfigBallot, Ballot ballot);

        /// <summary>
        /// </summary>
        /// <param name="key"></param>
        /// <param name="proposerConfigBallot">
        /// The ballot number for the configuration which the proposer is using, taken from <see cref="ReplicaSetConfiguration.Stamp"/>.
        /// </param>
        /// <param name="ballot"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        Task<AcceptResponse> Accept(string key, Ballot proposerConfigBallot, Ballot ballot, object value);

        /// <summary>
        /// Returns the list of keys which are present on this instance.
        /// </summary>
        /// <returns>The list of keys which are present on this instance.</returns>
        Task<List<string>> GetKeys();
    }
}