using System.IO;
using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.MetadataStore.Storage
{
    public class SimpleFileSystemStoreOptionsValidator : IConfigurationValidator
    {
        private readonly IOptions<SimpleFileSystemStoreOptions> options;
        public SimpleFileSystemStoreOptionsValidator(IOptions<SimpleFileSystemStoreOptions> options)
        {
            this.options = options;
        }

        public void ValidateConfiguration()
        {
            var dir = this.options.Value.Directory;
            if (string.IsNullOrWhiteSpace(dir))
            {
                throw new OrleansConfigurationException($"{nameof(SimpleFileSystemStoreOptions)}.{nameof(SimpleFileSystemStoreOptions.Directory)} must have a value.");
            }

            if (!Directory.GetParent(dir).Exists)
            {
                throw new OrleansConfigurationException($"The parent directory of {nameof(SimpleFileSystemStoreOptions)}.{nameof(SimpleFileSystemStoreOptions.Directory)} must exist.");
            }
        }
    }
}