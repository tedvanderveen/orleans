using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;


namespace Orleans.MetadataStore.Storage
{
    public class SimpleFileSystemStore : ILocalStore
    {
        private readonly SimpleFileSystemStoreOptions options;
        private readonly string tmpDirectory;
        private readonly string bakDirectory;
        private readonly string directory;

        public SimpleFileSystemStore(IOptions<SimpleFileSystemStoreOptions> options)
        {
            this.options = options.Value;
            this.directory = this.options.Directory;
            this.tmpDirectory = Path.Combine(directory, "tmp");
            this.bakDirectory = Path.Combine(directory, "bak");
            if (!Directory.Exists(this.directory)) Directory.CreateDirectory(this.directory);
            if (!Directory.Exists(this.tmpDirectory)) Directory.CreateDirectory(this.tmpDirectory);
            if (!Directory.Exists(this.bakDirectory)) Directory.CreateDirectory(this.bakDirectory);
        }

        public async Task<TValue> Read<TValue>(string key)
        {
            var path = Path.Combine(this.directory, key + ".json");
            if (!File.Exists(path)) return default(TValue);
            var fileText = await ReadFileAsync(path).ConfigureAwait(false);
            return JsonConvert.DeserializeObject<TValue>(fileText, this.options.JsonSettings);
        }

        public async Task Write<TValue>(string key, TValue value)
        {
            var fileText = JsonConvert.SerializeObject(value, this.options.JsonSettings);
            var fileName = key + ".json";
            var tmpFile = Path.Combine(this.tmpDirectory, fileName);
            var targetFile = Path.Combine(this.directory, fileName);
            var backupFile = Path.Combine(this.bakDirectory, fileName);
            await WriteFileAsync(targetFile, fileText);
            /*if (File.Exists(targetFile)) File.Replace(tmpFile, targetFile, backupFile);
            else File.Move(tmpFile, targetFile);*/
        }

        public Task<List<string>> GetKeys(int maxResults = 100, string afterKey = null)
        {
            var keys = Directory.EnumerateFiles(this.directory, "*.json").Select(f => f.Remove(f.Length - 5));
            return Task.FromResult(keys.ToList());
        }

        private static async Task WriteFileAsync(string path, string contents)
        {
            using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Write, 4096, FileOptions.Asynchronous))
            using (var writer = new StreamWriter(stream, Encoding.UTF8))
            {
                await writer.WriteAsync(contents);
                await writer.FlushAsync();
                writer.Close();
            }
        }

        private static async Task<string> ReadFileAsync(string path)
        {
            using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan))
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            {
                return await reader.ReadToEndAsync();
            }
        }
    }
}