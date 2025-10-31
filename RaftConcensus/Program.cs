using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RaftConcensus;
using Serilog;

//Log.Logger = new LoggerConfiguration().MinimumLevel.Debug().WriteTo..CreateLogger();

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseKestrel(options =>
{
    options.ListenAnyIP(50051, listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
});
builder.Services.AddGrpc();
builder.Services.AddSingleton<RaftNode>(sp =>
{
    var nodeId = Environment.GetEnvironmentVariable("NODE_ID") ?? "node1";
    var peers = Environment.GetEnvironmentVariable("PEERS")?.Split(',') ?? new[] { "10.0.1.4:50051", "10.1.1.4:50051" };  // Exclude self
    var azureConn = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
    return new RaftNode(nodeId, peers.ToList(), azureConn!, sp.GetRequiredService<ILogger<RaftNode>>());
});

var app = builder.Build();
app.MapGrpcService<RaftNode>();
app.Run();