using System;
using Microsoft.Extensions.Configuration;
using Orleans.Configuration;
using Xunit;
using Orleans.Hosting;
using Orleans.Logging;
using Orleans.TestingHost;
using Orleans.Transactions.Tests;
using Orleans.Transactions.Tests.DeactivationTransaction;
using TestExtensions;
using Tester;

namespace Orleans.Transactions.AzureStorage.Tests
{
    public class TestFixture : BaseTestClusterFixture
    {
        protected override void CheckPreconditionsOrThrow()
        {
            base.CheckPreconditionsOrThrow();
            TestUtils.CheckForAzureStorage();
        }

        protected override void ConfigureTestCluster(TestClusterBuilder builder)
        {
            builder.AddSiloBuilderConfigurator<Configurator>();
            builder.AddClientBuilderConfigurator<Configurator>();
        }

        public class Configurator : ISiloBuilderConfigurator, IClientBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                hostBuilder
                    .Configure<SiloMessagingOptions>(o => o.ResponseTimeoutWithDebugger = TimeSpan.FromSeconds(30))
                    .ConfigureTracingForTransactionTests()
                    .AddAzureTableTransactionalStateStorage(TransactionTestConstants.TransactionStore, options =>
                    {
                        options.ConnectionString = TestDefaultConfiguration.DataConnectionString;
                    })
                    .UseDistributedTM()
                    .ConfigureLogging(l => l.AddFile($"C:\\tmp\\tx-tests\\silo_{DateTime.Now.ToString("u").Replace(':', '-').Replace(' ', '_')}-{Guid.NewGuid().GetHashCode():X}.txt"));
            }

            public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
            {
                clientBuilder
                    .Configure<ClientMessagingOptions>(o => o.ResponseTimeoutWithDebugger = TimeSpan.FromSeconds(30))
                    .ConfigureLogging(l => l.AddFile($"C:\\tmp\\tx-tests\\client_{DateTime.Now.ToString("u").Replace(':', '-').Replace(' ', '_')}-{Guid.NewGuid().GetHashCode():X}.txt"));
            }
        }
    }

    public class DeactivationTestFixture : BaseTestClusterFixture
    {
        protected override void CheckPreconditionsOrThrow()
        {
            base.CheckPreconditionsOrThrow();
            TestUtils.CheckForAzureStorage();
        }

        protected override void ConfigureTestCluster(TestClusterBuilder builder)
        {
            builder.AddSiloBuilderConfigurator<SiloBuilderConfigurator>();
        }

        public class SiloBuilderConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                hostBuilder
                    .ConfigureTracingForTransactionTests()
                    .AddAzureTableTransactionalStateStorage(TransactionTestConstants.TransactionStore, options =>
                    {
                        options.ConnectionString = TestDefaultConfiguration.DataConnectionString;
                    })
                    .UseDeactivationTransactionState()
                    .UseDistributedTM();
            }
        }
    }

    public class SkewedClockTestFixture : TestFixture
    {
        protected override void ConfigureTestCluster(TestClusterBuilder builder)
        {
            builder.AddSiloBuilderConfigurator<SkewedClockConfigurator>();
            base.ConfigureTestCluster(builder);
        }
    }
}
