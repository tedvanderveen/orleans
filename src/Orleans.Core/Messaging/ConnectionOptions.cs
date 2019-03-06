using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Connections;

namespace Orleans.Configuration
{
    public class ConnectionOptions
    {
        private readonly ConnectionBuilderDelegates connectionBuilder = new ConnectionBuilderDelegates();

        public void ConfigureConnection(Action<IConnectionBuilder> configure) => this.connectionBuilder.Add(configure);

        internal void ConfigureConnectionBuilder(IConnectionBuilder builder) => this.connectionBuilder.Invoke(builder);

        internal class ConnectionBuilderDelegates
        {
            private readonly List<Action<IConnectionBuilder>> configurationDelegates = new List<Action<IConnectionBuilder>>();

            public void Add(Action<IConnectionBuilder> configure)
                => this.configurationDelegates.Add(configure ?? throw new ArgumentNullException(nameof(configure)));

            public void Invoke(IConnectionBuilder builder)
            {
                foreach (var configureDelegate in this.configurationDelegates)
                {
                    configureDelegate(builder);
                }
            }
        }
    }
}
