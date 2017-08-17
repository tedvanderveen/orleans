using System.IO;
using System.Linq;
using Microsoft.Build.Framework;
using MSBuildTask = Microsoft.Build.Utilities.Task;

namespace Microsoft.Orleans.CodeGenerator.MSBuild
{
    public class GenerateOrleansCode : MSBuildTask
    {
        [Required]
        public ITaskItem InputAssembly { get; set; }

        [Required]
        public ITaskItem OutputFileName { get; set; }

        [Required]
        public ITaskItem[] ReferencePaths { get; set; }

        public override bool Execute()
        {
            var inputAssembly = new FileInfo(this.InputAssembly.ItemSpec);
            var outputFileName = this.OutputFileName.ItemSpec;
            var referencedAssemblies = this.ReferencePaths.Select(item => item.ItemSpec).ToList();
            var generator = new CodeGenerationTask(
                new CodeGenOptions {InputAssembly = inputAssembly, OutputFileName = outputFileName, ReferencedAssemblies = referencedAssemblies},
                message => this.Log.LogMessage(MessageImportance.High, message));
            return generator.GenerateCode();
        }
    }
}
