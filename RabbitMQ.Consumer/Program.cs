/*
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks;

Console.WriteLine("=== RabbitMQ Consumer - Direct Exchange ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// Use async methods and 'await using' for proper disposal
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string exchangeName = "order.direct.exchange";
const string queuePayment = "order.queue.payment";
const string queueShipping = "order.queue.shipping";

// Declare queues asynchronously
await channel.QueueDeclareAsync(queue: queuePayment, durable: true, exclusive: false, autoDelete: false);
await channel.QueueDeclareAsync(queue: queueShipping, durable: true, exclusive: false, autoDelete: false);

// Payment Consumer configuration
var consumerPayment = new AsyncEventingBasicConsumer(channel);
consumerPayment.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [✓] Payment received: {message}");
    return Task.CompletedTask;
};

// Shipping Consumer configuration
var consumerShipping = new AsyncEventingBasicConsumer(channel);
consumerShipping.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [✓] Shipping received: {message}");
    return Task.CompletedTask;
};

// Start consuming from both queues asynchronously
await channel.BasicConsumeAsync(queue: queuePayment, autoAck: true, consumer: consumerPayment);
await channel.BasicConsumeAsync(queue: queueShipping, autoAck: true, consumer: consumerShipping);

Console.WriteLine(" [*] Consumer is listening to the payment and shipping queues...");
Console.WriteLine("Press any key to exit...");
Console.ReadKey();

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks;

Console.WriteLine("=== RabbitMQ Consumer - Fanout Exchange (Publish/Subscribe) ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// Use async methods and 'await using' for proper disposal
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string exchangeName = "order.fanout.exchange";
const string queueNotification = "order.queue.notification";
const string queueAnalytics = "order.queue.analytics";
const string queueShipping = "order.queue.shipping";

// Create queues for each service asynchronously
await channel.QueueDeclareAsync(queue: queueNotification, durable: true, exclusive: false, autoDelete: false);
await channel.QueueDeclareAsync(queue: queueAnalytics, durable: true, exclusive: false, autoDelete: false);
await channel.QueueDeclareAsync(queue: queueShipping, durable: true, exclusive: false, autoDelete: false);

// Bind all queues to the Fanout Exchange asynchronously
await channel.QueueBindAsync(queue: queueNotification, exchange: exchangeName, routingKey: string.Empty);
await channel.QueueBindAsync(queue: queueAnalytics, exchange: exchangeName, routingKey: string.Empty);
await channel.QueueBindAsync(queue: queueShipping, exchange: exchangeName, routingKey: string.Empty);

Console.WriteLine("All queues have been bound to the Fanout Exchange.");

// Notification Consumer configuration
var consumerNotification = new AsyncEventingBasicConsumer(channel);
consumerNotification.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [📢] Notification Service: {message}");
    return Task.CompletedTask;
};

// Analytics Consumer configuration
var consumerAnalytics = new AsyncEventingBasicConsumer(channel);
consumerAnalytics.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [📊] Analytics Service: {message}");
    return Task.CompletedTask;
};

// Shipping Consumer configuration
var consumerShipping = new AsyncEventingBasicConsumer(channel);
consumerShipping.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [🚚] Shipping Service: {message}");
    return Task.CompletedTask;
};

// Start consuming messages asynchronously
await channel.BasicConsumeAsync(queue: queueNotification, autoAck: true, consumer: consumerNotification);
await channel.BasicConsumeAsync(queue: queueAnalytics, autoAck: true, consumer: consumerAnalytics);
await channel.BasicConsumeAsync(queue: queueShipping, autoAck: true, consumer: consumerShipping);

Console.WriteLine(" [*] Three consumers are ready to receive broadcast messages...");
Console.WriteLine("Press any key to exit...");
Console.ReadKey();


using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks;

Console.WriteLine("=== RabbitMQ Consumer - Topic Exchange ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// Use async methods and 'await using' for proper disposal
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string exchangeName = "order.topic.exchange";

// 1. Proactively declare the exchange to prevent 404 errors if consumer starts first
await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Topic, durable: true);

// 2. Declare queues with different bindings asynchronously
await channel.QueueDeclareAsync(queue: "order.all.queue", durable: true, exclusive: false, autoDelete: false);
await channel.QueueDeclareAsync(queue: "order.payment.queue", durable: true, exclusive: false, autoDelete: false);
await channel.QueueDeclareAsync(queue: "order.shipping.queue", durable: true, exclusive: false, autoDelete: false);

// 3. Bind queues using different topic patterns asynchronously
await channel.QueueBindAsync(queue: "order.all.queue", exchange: exchangeName, routingKey: "#");                 // All events
await channel.QueueBindAsync(queue: "order.payment.queue", exchange: exchangeName, routingKey: "order.payment.*"); // Payment events only
await channel.QueueBindAsync(queue: "order.shipping.queue", exchange: exchangeName, routingKey: "order.shipped");  // Shipping events only

Console.WriteLine("Queues have been connected with Topic bindings.");

// Consumer for all events
var consumerAll = new AsyncEventingBasicConsumer(channel);
consumerAll.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [🌐] All Events: {message} | RoutingKey: {ea.RoutingKey}");
    return Task.CompletedTask;
};

// Payment consumer
var consumerPayment = new AsyncEventingBasicConsumer(channel);
consumerPayment.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [💰] Payment Service: {message}");
    return Task.CompletedTask;
};

// Shipping consumer
var consumerShipping = new AsyncEventingBasicConsumer(channel);
consumerShipping.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [🚚] Shipping Service: {message}");
    return Task.CompletedTask;
};

// 4. Start consuming messages asynchronously
await channel.BasicConsumeAsync(queue: "order.all.queue", autoAck: true, consumer: consumerAll);
await channel.BasicConsumeAsync(queue: "order.payment.queue", autoAck: true, consumer: consumerPayment);
await channel.BasicConsumeAsync(queue: "order.shipping.queue", autoAck: true, consumer: consumerShipping);

Console.WriteLine(" [*] Consumers are active with Topic-based routing patterns...");
Console.WriteLine("Press any key to exit...");
Console.ReadKey();


using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks;

Console.WriteLine("=== RabbitMQ Consumer - Publisher Confirms Demo ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// Use async methods and 'await using' for proper disposal
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string queueName = "order.confirm.queue";

// Queue declaration is now async
await channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);

// Use AsyncEventingBasicConsumer for v7
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [✓] Message received: {message}");

    // Return Task.CompletedTask because we are not awaiting anything inside this lambda
    return Task.CompletedTask;
};

// BasicConsume is now BasicConsumeAsync
await channel.BasicConsumeAsync(queue: queueName, autoAck: true, consumer: consumer);

Console.WriteLine(" [*] Consumer is ready to receive messages...");
Console.WriteLine("Press any key to exit...");
Console.ReadKey();

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

Console.WriteLine("=== RabbitMQ Consumer - Manual Ack / Nack / Requeue ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// Use async methods and 'await using' for proper disposal
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string queueName = "order.ack.queue";

// Proactively declare the queue asynchronously
await channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);

// Use AsyncEventingBasicConsumer for v7
var consumer = new AsyncEventingBasicConsumer(channel);

// Marked lambda as 'async' because we will await acknowledgement methods inside
consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    try
    {
        Console.WriteLine($" [📥] Message received: {message}");

        // Simulate processing
        if (message.Contains("error", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine(" [✗] Processing failed! Message will be rejected.");

            // Reject + Requeue = Return the message to the queue (Async in v7)
            await channel.BasicRejectAsync(deliveryTag: ea.DeliveryTag, requeue: true);
            return;
        }

        // Successful processing
        Console.WriteLine(" [✓] Processing completed successfully.");

        // Manual Ack (Async in v7)
        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    catch (Exception ex)
    {
        Console.WriteLine($" [⚠] Unexpected error: {ex.Message}");

        // Nack without Requeue (Async in v7)
        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
    }
};

// Important: Set autoAck to false to use Manual Acknowledgements
await channel.BasicConsumeAsync(queue: queueName,
                                autoAck: false,
                                consumer: consumer);

Console.WriteLine(" [*] Consumer started with Manual Acknowledgement.");
Console.WriteLine("Tip: Include the word 'error' in a message to simulate a processing failure.");
Console.WriteLine("Press any key to exit...");
Console.ReadKey();

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;

Console.WriteLine("=== RabbitMQ Consumer - Dead Letter Exchange ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// Use async methods and 'await using' for proper disposal
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

// ====================== Dead Letter Exchange Configuration ======================
const string dlxExchange = "order.dlx.exchange";
const string dlqQueue = "order.deadletter.queue";

// Topology declarations are now async
await channel.ExchangeDeclareAsync(exchange: dlxExchange, type: ExchangeType.Direct, durable: true);
await channel.QueueDeclareAsync(queue: dlqQueue, durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync(queue: dlqQueue, exchange: dlxExchange, routingKey: "order.failed");

// ====================== Main Queue with DLX ======================
const string mainQueue = "order.main.queue";

// RabbitMQ.Client v7 handles arguments using Dictionary<string, object?>
var mainQueueArgs = new Dictionary<string, object?>
{
    { "x-dead-letter-exchange", dlxExchange },
    { "x-dead-letter-routing-key", "order.failed" }
};

await channel.QueueDeclareAsync(queue: mainQueue,
                                durable: true,
                                exclusive: false,
                                autoDelete: false,
                                arguments: mainQueueArgs);

Console.WriteLine("Dead Letter Exchange and Queue have been configured.");

// ====================== Consumer ======================
// Use AsyncEventingBasicConsumer for v7
var consumer = new AsyncEventingBasicConsumer(channel);

// Mark lambda as 'async' to allow awaiting acknowledgement methods inside
consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    try
    {
        Console.WriteLine($" [📥] Received: {message}");

        if (message.Contains("error", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine(" [✗] Processing failed - message will be sent to the Dead Letter Queue.");

            // BasicNack is now BasicNackAsync. Requeue: false drops it into the DLX route.
            await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
            return;
        }

        Console.WriteLine(" [✓] Processed successfully.");

        // BasicAck is now BasicAckAsync
        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    catch (Exception ex)
    {
        Console.WriteLine($" [⚠] Error: {ex.Message}");
        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
    }
};

// Start consuming messages asynchronously
await channel.BasicConsumeAsync(queue: mainQueue, autoAck: false, consumer: consumer);

Console.WriteLine(" [*] Main consumer is running.");
Console.WriteLine("Tip: To test the DLQ, include the word 'error' in a message.");
Console.WriteLine("Press any key to exit...");
Console.ReadKey();
*/
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;

Console.WriteLine("=== RabbitMQ Consumer - Retry Mechanism with Backoff ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

// ====================== Retry Dead Letter Configuration ======================
const string retryExchange = "order.retry.exchange";
const string retryQueue = "order.retry.queue";
const string dlxExchange = "order.dlx.exchange";
const string dlqQueue = "order.deadletter.queue";

var retryArgs = new Dictionary<string, object?>
{
    { "x-dead-letter-exchange", "order.main.exchange" },
    { "x-dead-letter-routing-key", "order.process" },
    { "x-message-ttl", 10000 }   // 10-second delay before retry
};

await channel.ExchangeDeclareAsync(retryExchange, ExchangeType.Direct, durable: true);
await channel.QueueDeclareAsync(retryQueue, durable: true, exclusive: false, autoDelete: false, arguments: retryArgs);
await channel.QueueBindAsync(retryQueue, retryExchange, "order.retry");

await channel.ExchangeDeclareAsync(dlxExchange, ExchangeType.Direct, durable: true);
await channel.QueueDeclareAsync(dlqQueue, durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync(dlqQueue, dlxExchange, "order.failed");

// ====================== Main Queue ======================
var mainQueueArgs = new Dictionary<string, object?>
{
    { "x-dead-letter-exchange", retryExchange },
    { "x-dead-letter-routing-key", "order.retry" }
};

const string mainQueue = "order.main.queue";
await channel.QueueDeclareAsync(mainQueue, durable: true, exclusive: false, autoDelete: false, arguments: mainQueueArgs);

int maxRetries = 3;

// ====================== Consumer ======================
var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    int retryCount = 0;
    if (ea.BasicProperties.Headers != null && ea.BasicProperties.Headers.TryGetValue("x-retry-count", out var value))
    {
        if (value is int intValue) retryCount = intValue;
        else if (value is long longValue) retryCount = (int)longValue;
    }

    try
    {
        Console.WriteLine($" [📥] Attempt {retryCount + 1} - Message: {message}");

        if (message.Contains("error", StringComparison.OrdinalIgnoreCase))
        {
            if (retryCount < maxRetries - 1)
            {
                Console.WriteLine($" [⚠] Processing failed - retrying in 10 seconds (Retry Count: {retryCount + 1})");

                var props = new BasicProperties
                {
                    Headers = new Dictionary<string, object?> { { "x-retry-count", retryCount + 1 } }
                };

                // FIXED: Included mandatory parameter
                await channel.BasicPublishAsync(exchange: retryExchange,
                                                routingKey: "order.retry",
                                                mandatory: false,
                                                basicProperties: props,
                                                body: body);
            }
            else
            {
                Console.WriteLine(" [☠] Maximum retry attempts reached. Message moved to the DLQ.");
                await channel.BasicPublishAsync(exchange: dlxExchange, routingKey: "order.failed", body: body);
            }

            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            return;
        }

        Console.WriteLine(" [✓] Processed successfully.");
        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    catch (Exception ex)
    {
        Console.WriteLine($" [✗] Error: {ex.Message}");

        if (retryCount < maxRetries - 1)
        {
            var props = new BasicProperties
            {
                Headers = new Dictionary<string, object?> { { "x-retry-count", retryCount + 1 } }
            };

            // FIXED: Included mandatory parameter
            await channel.BasicPublishAsync(exchange: retryExchange,
                                            routingKey: "order.retry",
                                            mandatory: false,
                                            basicProperties: props,
                                            body: body);
        }
        else
        {
            Console.WriteLine(" [☠] Maximum retry attempts reached due to exception. Message moved to the DLQ.");
            await channel.BasicPublishAsync(exchange: dlxExchange, routingKey: "order.failed", body: body);
        }

        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
};

await channel.BasicConsumeAsync(queue: mainQueue, autoAck: false, consumer: consumer);

Console.WriteLine(" [*] Retry Mechanism enabled (maximum 3 attempts).");
Console.WriteLine("To test retries, include the word 'error' in the message.");
Console.ReadLine();