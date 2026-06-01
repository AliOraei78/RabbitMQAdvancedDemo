using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Threading;
using System.Threading.Tasks;

public class RabbitMQConnectionManager : IAsyncDisposable
{
    private readonly ConnectionFactory _factory;
    private IConnection? _connection;
    private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
    private bool _disposed = false;

    public RabbitMQConnectionManager()
    {
        _factory = new ConnectionFactory()
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest",
            AutomaticRecoveryEnabled = true,      // Automatic recovery is built into v7
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
            RequestedHeartbeat = TimeSpan.FromSeconds(30)
        };
    }

    // Changed return type from IModel to Task<IChannel>
    public async Task<IChannel> CreateChannelAsync()
    {
        await _lock.WaitAsync();
        try
        {
            if (_connection == null || !_connection.IsOpen)
            {
                Console.WriteLine("Creating a new RabbitMQ connection...");

                // Connection creation is now async
                _connection = await _factory.CreateConnectionAsync();

                // Hooking into the async event handler for shutdown
                _connection.ConnectionShutdownAsync += async (sender, args) =>
                {
                    Console.WriteLine($"Connection lost! Code: {args.ReplyCode} | Reason: {args.ReplyText}");
                    await Task.CompletedTask;
                };
            }

            // CreateChannelAsync replaces CreateModel()
            return await _connection.CreateChannelAsync();
        }
        finally
        {
            _lock.Release();
        }
    }

    // Replaced sync Dispose with async DisposeAsync for network stream teardown
    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            if (_connection != null)
            {
                await _connection.CloseAsync();
                await _connection.DisposeAsync();
            }

            _lock.Dispose();
            _disposed = true;
        }
    }
}