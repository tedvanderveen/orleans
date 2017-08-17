using System.Collections.Generic;
using System.IO;

namespace Microsoft.Orleans.CodeGenerator.MSBuild
{
    public class CodeGenOptions
    {
        public FileInfo InputAssembly;

        public List<string> ReferencedAssemblies = new List<string>();

        public string OutputFileName;
    }
}