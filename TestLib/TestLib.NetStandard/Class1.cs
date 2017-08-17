using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans;

namespace TestLib.NetStandard
{
    public interface IMyGrain : IGrainWithGuidKey
    {
        Task SomeTask(int num);
    }

    [Serializable]
    public class MySerializable
    {
        [JsonProperty("hi")]
        public JValue JVal { get; set; }
    }
}
