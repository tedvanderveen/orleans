using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using Microsoft.Extensions.DependencyModel;
using Microsoft.Extensions.DependencyModel.Resolution;

namespace Microsoft.Orleans.CodeGenerator.MSBuild
{
    /// <summary>
    /// Simple class that loads the reference assemblies upon the AppDomain.AssemblyResolve
    /// </summary>
    internal class AssemblyResolver
    {
        private readonly Action<string> log;

        /// <summary>
        /// Needs to be public so can be serialized accross the the app domain.
        /// </summary>
        public Dictionary<string, string> ReferenceAssemblyPaths { get; } = new Dictionary<string, string>();
        
        private readonly ICompilationAssemblyResolver assemblyResolver;

        private readonly DependencyContext dependencyContext;
        private readonly AssemblyLoadContext loadContext;

        public AssemblyResolver(string path, List<string> referencedAssemblies, Action<string> log)
        {
            this.log = log;
            if (Path.GetFileName(path) == "Orleans.dll")  this.Assembly = Assembly.Load("Orleans");
            else this.Assembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(path);
            this.dependencyContext = DependencyContext.Load(this.Assembly);

            this.assemblyResolver = new CompositeCompilationAssemblyResolver(
                new ICompilationAssemblyResolver[]
                {
                    new AppBaseCompilationAssemblyResolver(Path.GetDirectoryName(path)),
                    new ReferenceAssemblyPathResolver(),
                    new PackageCompilationAssemblyResolver()
                });

            this.loadContext = AssemblyLoadContext.GetLoadContext(this.Assembly);
            this.loadContext.Resolving += this.AssemblyLoadContextResolving;
            if (this.loadContext != AssemblyLoadContext.Default)
            {
                AssemblyLoadContext.Default.Resolving += this.AssemblyLoadContextResolving;
            }

            AppDomain.CurrentDomain.AssemblyResolve += this.ResolveAssembly;

            var runtimeLibraries = this.dependencyContext?.RuntimeLibraries;
            if (runtimeLibraries == null)
            {
                this.log($"DependencyContext not found for {path}.");
            }
            else
            {
                foreach (var lib in runtimeLibraries)
                {
                    this.log($"Dep: {lib.Name} = {lib.Path}");
                }
            }

            foreach (var assemblyPath in referencedAssemblies)
            {
                var libName = Path.GetFileNameWithoutExtension(assemblyPath);
                this.ReferenceAssemblyPaths[libName] = assemblyPath;
                var asmName = AssemblyName.GetAssemblyName(assemblyPath);
                this.ReferenceAssemblyPaths[asmName.FullName] = assemblyPath;

/*                try
                {
                    this.loadContext.LoadFromAssemblyPath(assemblyPath);
                }
                catch (Exception exception)
                {
                    this.log($"Failed to load {assemblyPath}: {exception}");
                }*/

            }

            /*            var libraries =
                            this.dependencyContext?.RuntimeLibraries;
                        if (libraries != null)
                        {
                            foreach (var library in libraries)
                            {
                                var wrapper = new CompilationLibrary(
                                    library.Type,
                                    library.Name,
                                    library.Version,
                                    library.Hash,
                                    library.RuntimeAssemblyGroups.SelectMany(g => g.AssetPaths),
                                    library.Dependencies,
                                    library.Serviceable);

                                var assemblies = new List<string>();
                                this.assemblyResolver.TryResolveAssemblyPaths(wrapper, assemblies);
                                foreach (var asm in assemblies)
                                {
                                    try
                                    {
                                        this.loadContext.LoadFromAssemblyPath(asm);
                                        break;
                                    }
                                    catch (Exception exception)
                                    {
                                        this.log($"Failed to load asm from path {asm}: {exception}");
                                    }
                                }
                            }
                        }*/
        }

        public Assembly Assembly { get; }

        public void Dispose()
        {
            this.loadContext.Resolving -= this.AssemblyLoadContextResolving;
            AssemblyLoadContext.Default.Resolving -= this.AssemblyLoadContextResolving;
            AppDomain.CurrentDomain.AssemblyResolve -= this.ResolveAssembly;
        }

        /// <summary>
        /// Handles System.AppDomain.AssemblyResolve event of an System.AppDomain
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="args">The event data.</param>
        /// <returns>The assembly that resolves the type, assembly, or resource; 
        /// or null if theassembly cannot be resolved.
        /// </returns>
        public Assembly ResolveAssembly(object sender, ResolveEventArgs args)
        {
            this.log($"ResolveAssembly {args.Name}");
            return this.AssemblyLoadContextResolving(AssemblyLoadContext.GetLoadContext(args.RequestingAssembly), new AssemblyName(args.Name));
        }

        public Assembly AssemblyLoadContextResolving(AssemblyLoadContext context, AssemblyName name)
        {
            this.log($"AssemblyLoadContextResolving {name}");

            bool NamesMatch(RuntimeLibrary runtime)
            {
                return string.Equals(runtime.Name, name.Name, StringComparison.OrdinalIgnoreCase);
            }

            try
            {
                var library = this.dependencyContext?.RuntimeLibraries?.FirstOrDefault(NamesMatch);
                if (library != null)
                {
                    var wrapper = new CompilationLibrary(
                        library.Type,
                        library.Name,
                        library.Version,
                        library.Hash,
                        library.RuntimeAssemblyGroups.SelectMany(g => g.AssetPaths),
                        library.Dependencies,
                        library.Serviceable);

                    var assemblies = new List<string>();
                    this.assemblyResolver.TryResolveAssemblyPaths(wrapper, assemblies);
                    foreach (var asm in assemblies)
                    {
                        try
                        {
                            return this.loadContext.LoadFromAssemblyPath(asm);
                        }
                        catch (Exception exception)
                        {
                            this.log($"Failed to load asm {name} from path {asm}: {exception}");
                        }
                    }
                }

                Assembly assembly = null;
                string path;
                if (this.ReferenceAssemblyPaths.TryGetValue(name.FullName, out path)) assembly = this.loadContext.LoadFromAssemblyPath(path);
                else if (this.ReferenceAssemblyPaths.TryGetValue(name.Name, out path)) assembly = this.loadContext.LoadFromAssemblyPath(path);
                else this.log($"Could not resolve {name.Name}");
                return assembly;
            }
            catch (Exception exception)
            {
                this.log($"Exception in AssemblyLoadContextResolving for assembly {name}: {exception}");
                throw;
            }
        }
    }
}