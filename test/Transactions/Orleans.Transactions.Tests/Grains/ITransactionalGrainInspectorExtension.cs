using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Transactions.Tests.Grains
{
    public interface ITransactionalGrainInspectorExtension : IGrainExtension
    {
        [AlwaysInterleave]
        Task<string> GetActivationId();
    }

    public class TransactionalGrainInspectorExtension : ITransactionalGrainInspectorExtension
    {
        private readonly IGrainActivationContext context;
        private static readonly FieldInfo ActivationDataFieldInfo = typeof(Grain).GetField("Data", BindingFlags.NonPublic | BindingFlags.Instance);

        public TransactionalGrainInspectorExtension(IGrainActivationContext context)
        {
            this.context = context;
        }

        public Task<string> GetActivationId()
        {
            var grain = this.context.GrainInstance;
            var activationData = ActivationDataFieldInfo.GetValue(grain);
            return Task.FromResult(activationData.ToString());
        }
    }
}
