using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Orleans;
using Orleans.Hosting;
using UnitTests.GrainInterfaces;
using Xunit;

namespace NonSilo.Tests
{
    /// <summary>
    /// Tests for hosting via <see cref="Microsoft.Extensions.Hosting.IHostBuilder"/>.
    /// </summary>
    [TestCategory("BVT")]
    public class GenericHostTests
    {
        /// <summary>
        /// Tests that hosts created via <see cref="IHostBuilder"/> can be built, started, and can respond to client requests.
        /// </summary>
        [Fact]
        public async Task GenericHost_Starts()
        {

// TODO: don't merge this without selecting available ports here - fixed ports are bad for tests.
            var host = new HostBuilder()
                .AddOrleans(builder => builder.UseLocalhostClustering().AddMemoryGrainStorage("MemoryStore"))
                .Build();
            await host.StartAsync();

            var client = new ClientBuilder().UseLocalhostClustering().Build();
            await client.Connect(async ex =>
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                return true;
            });


            var result = await client.GetGrain<IEchoGrain>(Guid.NewGuid()).Echo("hi");
            Assert.Equal("hi", result);
        }

        /// <summary>
        /// Tests that the <see cref="ISiloHostBuilder.Build"/> method throws when used in conjunction with <see cref="IHostBuilder"/>.
        /// </summary>
        [Fact]
        public void GenericHost_SiloHostBuilder_Build_NotSupported()
        {
            Assert.Throws<NotSupportedException>(() => new HostBuilder().AddOrleans(orleans => orleans.Build()));
        }
    }
}