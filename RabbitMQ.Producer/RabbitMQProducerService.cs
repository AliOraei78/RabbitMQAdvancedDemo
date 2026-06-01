using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading.Tasks;

public class RabbitMQProducerService : IAsyncDisposable
{
    private readonly RabbitMQConnectionManager _connectionManager;
    private readonly IChannel _channel; // IModel renamed to IChannel
    private readonly string _exchangeName;
    private readonly string _routingKey;

    // Private constructor prevents standard instantiation, forcing the use of the Async Factory
    private RabbitMQProducerService(RabbitMQConnectionManager connectionManager, IChannel channel, string exchangeName, string routingKey)
    {
        _connectionManager = connectionManager;
        _channel = channel;
        _exchangeName = exchangeName;
        _routingKey = routingKey;
    }

    // Asynchronous Factory Method to handle async connection and topology declaration
    public static async Task<RabbitMQProducerService> CreateAsync(string exchangeName, string routingKey)
    {
        var connectionManager = new RabbitMQConnectionManager();

        // Await the asynchronous channel creation
        var channel = await connectionManager.CreateChannelAsync();

        // Topology setup is fully async in v7
        await channel.ExchangeDeclareAsync(exchange: exchangeName,
                                           type: ExchangeType.Direct,
                                           durable: true);

        return new RabbitMQProducerService(connectionManager, channel, exchangeName, routingKey);
    }

    // Changed signature to async Task to support BasicPublishAsync
    public async Task PublishAsync(string message)
    {
        try
        {
            var body = Encoding.UTF8.GetBytes(message);

            // Instantiate BasicProperties directly (CreateBasicProperties() is removed)
            var properties = new BasicProperties
            {
                DeliveryMode = DeliveryModes.Persistent // Replaces Persistent = true
            };

            // BasicPublish is now BasicPublishAsync (mandatory flag required when passing custom properties)
            await _channel.BasicPublishAsync(exchange: _exchangeName,
                                             routingKey: _routingKey,
                                             mandatory: false,
                                             basicProperties: properties,
                                             body: body);

            Console.WriteLine($" [✓] Message sent: {message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($" [✗] Error while publishing message: {ex.Message}");
        }
    }

    // Implemented IAsyncDisposable for clean, non-blocking hardware/network stream disposal
    public async ValueTask DisposeAsync()
    {
        if (_channel != null)
        {
            await _channel.DisposeAsync();
        }
        if (_connectionManager != null)
        {
            await _connectionManager.DisposeAsync();
        }
    }
}