---
layout: page
title: Hello, World!
---

# Hello, World!

The *HelloWorld* sample demonstrates how to write a grain, host it in a silo, and call it from a client. Source code for this sample is available in the [Samples/2.0/HelloWorld](https://github.com/dotnet/orleans/tree/master/Samples/2.0/HelloWorld) directory.

There are three projects involved:

* *SiloHost* hosts the silo and a greeter grain in a console application
* *OrleansClient* hosts the client which calls that grain, and is also a console application
* *HelloWorld.Interfaces* is referenced by both *SiloHost* &amp; *OrleansClient* and contains the interface they use to communicate.

Our client will communicate to our greeter grain using the interface defined in `IHelloGrain.cs`:

``` csharp
public interface IHelloGrain : Orleans.IGrainWithIntegerKey
{
    Task<string> SayHello(string greeting);
}
```

Note that `SayHello` returns `Task<string>`. All grain methods must return a `Task` or `Task<T>` since communication is asynchronous.

Our interface is implemented in `HelloGrain.cs` in the *SiloHost* project:

``` csharp
public class HelloGrain : Orleans.Grain, IHelloGrain
{
    public Task<string> SayHello(string greeting)
    {
        return Task.FromResult($"You said: '{greeting}', I say: Hello!");
    }
}
```

`HelloGrain` inherits from an Orleans-defined base class, `Grain`, and implements the communication interface defined above. Since there is nothing that the grain needs to wait on, the method is not declared `async` and instead returns its value using `Task.FromResult()`.

Grains are hosted in silos. The *SiloHost* project configures a silo which listens on `localhost` and hosts our `HelloGrain`.

``` csharp
// Configure a new silo which listens on localhost.
var host = new SiloHostBuilder()
    .UseLocalhostClustering()
    .ConfigureLogging(logging => logging.AddConsole())
    .Build();

await host.StartAsync();

Console.WriteLine("Press Enter to terminate...");
Console.ReadLine();

await host.StopAsync();
```

This configures and starts the silo which will operate until the user terminates it.

The client is found in *OrleansClient* project.

``` csharp
// Create a client which connects to the localhost cluster.
// Clients are thread safe and should live for the duration of your application.
var client = new ClientBuilder()
    .UseLocalhostClustering()
    .ConfigureLogging(logging => logging.AddConsole())
    .Build();

// In case the cluster is not ready when the client starts, we
// can provide a retry function. This one logs the message and
// retries after a delay.
async Task<bool> RetryFilter(Exception ex)
{
    Console.WriteLine($"Error while connecting to cluster: {ex}");
    await Task.Delay(TimeSpan.FromSeconds(5));
    return true;
}

// Connect to the cluster.
await client.Connect(RetryFilter);

// Get a reference to the grain.
var friend = client.GetGrain<IHelloGrain>(0);

// Call the grain and await its response.
var response = await friend.SayHello("Good morning, my friend!");

Console.WriteLine("\n\n{0}\n\n", response);
Console.ReadKey();

// Disconnect from the cluster when your application shuts down.
await client.Close();
```

The client sends a greeting message to the `IHelloGrain` grain on the silo and prints its response to the console.

## Running the sample

From Visual Studio, you can start start the *SiloHost* and *OrleansClient* projects simultaneously. You can set up multiple startup projects by right-clicking the solution in the Solution Explorer, and select `Set StartUp projects`.

Alternatively, you can run from the command line:

To start the silo:
```
dotnet run --project .\src\SiloHost
```

To start the client (you will have to use a different command window):
```
dotnet run --project .\src\OrleansClient
```