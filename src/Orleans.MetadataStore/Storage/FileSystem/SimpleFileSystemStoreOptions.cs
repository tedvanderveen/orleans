using Newtonsoft.Json;

namespace Orleans.MetadataStore.Storage
{
    public class SimpleFileSystemStoreOptions
    {
        public string Directory { get; set; }

        public JsonSerializerSettings JsonSettings { get; set; }
    }
}