using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading.Tasks;

public class RabbitMQConsumerService : IAsyncDisposable
{
    private readonly RabbitMQConnectionManager _connectionManager;
    private readonly IChannel _channel; // IModel renamed to IChannel
    private readonly string _queueName;
    private readonly string _serviceName;
    private readonly int _prefetchCount;

    // Private constructor blocks unsafe synchronous initialization
    private RabbitMQConsumerService(
        RabbitMQConnectionManager connectionManager,
        IChannel channel,
        string queueName,
        string serviceName,
        int prefetchCount)
    {
        _connectionManager = connectionManager;
        _channel = channel;
        _queueName = queueName;
        _serviceName = serviceName;
        _prefetchCount = prefetchCount;
    }

    // Asynchronous Factory Method handles connection, QoS pacing, and topology setup safely
    public static async Task<RabbitMQConsumerService> CreateAsync(
        string queueName,
        string serviceName,
        int prefetchCount = 5)
    {
        var connectionManager = new RabbitMQConnectionManager();
        var channel = await connectionManager.CreateChannelAsync();

        // BasicQos is now BasicQosAsync
        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: (ushort)prefetchCount,
            global: false);

        // QueueDeclare is now QueueDeclareAsync
        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false);

        return new RabbitMQConsumerService(connectionManager, channel, queueName, serviceName, prefetchCount);
    }

    // Signatures executing broker communication are refactored to async Task
    public async Task StartConsumingAsync()
    {
        // Migrate to AsyncEventingBasicConsumer for v7 asynchronous context handling
        var consumer = new AsyncEventingBasicConsumer(_channel);

        // Wire event logic into ReceivedAsync with an async lambda
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            try
            {
                Console.WriteLine(
                    $" [{_serviceName}] 📥 Message received: {message}");

                // Use non-blocking Task.Delay instead of freezing the thread pool
                await Task.Delay(800);

                // Simulate a failure in the Payment Service
                if (_serviceName.Contains("Payment") && message.Contains("#5"))
                {
                    throw new Exception(
                        "Payment processing error - transaction failed");
                }

                Console.WriteLine(
                    $" [{_serviceName}] ✅ Processed successfully");

                // BasicAck is now BasicAckAsync
                await _channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $" [{_serviceName}] ❌ Error: {ex.Message}");

                // BasicNack is now BasicNackAsync
                await _channel.BasicNackAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false,
                    requeue: false);
            }
        };

        // BasicConsume is now BasicConsumeAsync
        await _channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false,
            consumer: consumer);

        Console.WriteLine(
            $" [{_serviceName}] 🚀 Consumer started - Prefetch Count: {_prefetchCount}");
    }

    // Implemented IAsyncDisposable for modern channel and link closing sequences
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