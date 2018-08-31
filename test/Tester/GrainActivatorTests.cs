using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Runtime;
using Orleans.TestingHost;
using TestExtensions;
using UnitTests.GrainInterfaces;
using UnitTests.Grains;
using Xunit;
using Orleans.Hosting;

namespace UnitTests.General
{
    public class OneWayDeactivationTests : OrleansTestingBase, IClassFixture<OneWayDeactivationTests.Fixture>
    {
        private readonly Fixture fixture;

        public class Fixture : BaseTestClusterFixture
        {
            protected override void ConfigureTestCluster(TestClusterBuilder builder)
            {
                builder.Options.InitialSilosCount = 3;
            }
        }

        public OneWayDeactivationTests(Fixture fixture)
        {
            this.fixture = fixture;
        }

        /// <summary>
        /// Tests that calling [OneWay] methods on an activation which no longer exists triggers a cache invalidation.
        /// Subsequent calls should reactivate the grain.
        /// </summary>
        [Fact]
        public async Task OneWay_Deactivation_CacheInvalidated()
        {
            IOneWayGrain grainToCallFrom;
            while (true)
            {
                grainToCallFrom = this.fixture.Client.GetGrain<IOneWayGrain>(Guid.NewGuid());
                var grainHost = await grainToCallFrom.GetSiloAddress();
                if (grainHost.Equals(this.fixture.HostedCluster.Primary.SiloAddress))
                {
                    break;
                }
            }

            var grainToDeactivate = await grainToCallFrom.GetOtherGrain();

            // Activate the grain.
            var initialActivationId = await grainToDeactivate.GetActivationId();
            await grainToCallFrom.NotifyOtherGrain();

            // Deactivate the grain.
            var siloToKill = await grainToDeactivate.GetPrimaryForGrain();
            var silo = this.fixture.HostedCluster.Silos.First(s => s.SiloAddress.Equals(siloToKill));
            silo.StopSilo(false);
            await Task.Delay(TimeSpan.FromSeconds(30));

            Task task;
            Task notified;
            var remainingAttempts = 10;
            do
            {
                // Make a OneWay call on the grain.
                notified = grainToCallFrom.NotifyOtherGrain();

                // Check if the call successfully reached the grain
                task = await Task.WhenAny(notified, Task.Delay(TimeSpan.FromSeconds(1)));
                Assert.True(--remainingAttempts > 0);
            } while (task != notified);

            var finalActivationId = await grainToDeactivate.GetActivationId();
            Assert.NotEqual(initialActivationId, finalActivationId);
        }
    }

    [TestCategory("DI")]
    public class GrainActivatorTests : OrleansTestingBase, IClassFixture<GrainActivatorTests.Fixture>
    {
        private readonly Fixture fixture;

        public class Fixture : BaseTestClusterFixture
        {
            protected override void ConfigureTestCluster(TestClusterBuilder builder)
            {
                builder.Options.InitialSilosCount = 1;
                builder.AddSiloBuilderConfigurator<TestSiloBuilderConfigurator>();
            }

            private class TestSiloBuilderConfigurator : ISiloBuilderConfigurator
            {
                public void Configure(ISiloHostBuilder hostBuilder)
                {
                    hostBuilder.ConfigureServices(services =>
                        services.Replace(ServiceDescriptor.Singleton(typeof(IGrainActivator), typeof(HardcodedGrainActivator))));
                }
            }
        }

        public GrainActivatorTests(Fixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional")]
        public async Task CanUseCustomGrainActivatorToCreateGrains()
        {
            ISimpleDIGrain grain = this.fixture.GrainFactory.GetGrain<ISimpleDIGrain>(GetRandomGrainId(), grainClassNamePrefix: "UnitTests.Grains.ExplicitlyRegistered");
            var actual = await grain.GetStringValue();
            Assert.Equal(HardcodedGrainActivator.HardcodedValue, actual);
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional")]
        public async Task CanUseCustomGrainActivatorToReleaseGrains()
        {
            ISimpleDIGrain grain1 = this.fixture.GrainFactory.GetGrain<ISimpleDIGrain>(GetRandomGrainId(), grainClassNamePrefix: "UnitTests.Grains.ExplicitlyRegistered");
            long initialReleasedInstances = await grain1.GetLongValue();

            ISimpleDIGrain grain2 = this.fixture.GrainFactory.GetGrain<ISimpleDIGrain>(GetRandomGrainId(), grainClassNamePrefix: "UnitTests.Grains.ExplicitlyRegistered");
            long secondReleasedInstances = await grain2.GetLongValue();

            Assert.Equal(initialReleasedInstances, secondReleasedInstances);

            await grain1.DoDeactivate();
            await Task.Delay(250);

            ISimpleDIGrain grain3 = this.fixture.GrainFactory.GetGrain<ISimpleDIGrain>(GetRandomGrainId(), grainClassNamePrefix: "UnitTests.Grains.ExplicitlyRegistered");
            long finalReleasedInstances = await grain3.GetLongValue();
            Assert.Equal(initialReleasedInstances + 1, finalReleasedInstances);
        }

        private class HardcodedGrainActivator : DefaultGrainActivator, IGrainActivator
        {
            public const string HardcodedValue = "Hardcoded Test Value";
            private int numberOfReleasedInstances;
            public HardcodedGrainActivator(IServiceProvider service) : base(service)
            {
            }

            public override object Create(IGrainActivationContext context)
            {
                if (context.GrainType == typeof(ExplicitlyRegisteredSimpleDIGrain))
                {
                    return new ExplicitlyRegisteredSimpleDIGrain(new InjectedService(NullLoggerFactory.Instance), HardcodedValue, numberOfReleasedInstances);
                }

                return base.Create(context);
            }

            public override void Release(IGrainActivationContext context, object grain)
            {
                if (context.GrainType == typeof(ExplicitlyRegisteredSimpleDIGrain))
                {
                    numberOfReleasedInstances++;
                }
                else
                {
                    base.Release(context, grain);
                }
            }
        }
    }
}
