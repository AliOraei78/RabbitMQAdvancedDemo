/*
using RabbitMQ.Client;
using System.Text;

Console.WriteLine("=== RabbitMQ Producer - Fanout Exchange (Publish/Subscribe) ===");

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

// ExchangeDeclare is now ExchangeDeclareAsync
await channel.ExchangeDeclareAsync(exchange: exchangeName,
                                   type: ExchangeType.Fanout,
                                   durable: true);

Console.WriteLine($"Fanout Exchange created: {exchangeName}");

int messageCount = 0;

while (true)
{
    Console.WriteLine("\nEnter a message (or 'exit' to quit):");
    string? input = Console.ReadLine();

    if (input?.ToLower() == "exit") break;
    if (string.IsNullOrWhiteSpace(input)) continue;

    messageCount++;
    string message = $"Order Event #{messageCount}: {input} - Time: {DateTime.Now:HH:mm:ss}";
    var body = Encoding.UTF8.GetBytes(message);

    // BasicPublish is now BasicPublishAsync
    await channel.BasicPublishAsync(exchange: exchangeName,
                                    routingKey: string.Empty, // Ignored in Fanout exchanges
                                    body: body);

    Console.WriteLine($" [x] Message broadcast to all subscribers: {message}");
}

Console.WriteLine("Producer stopped.");


using RabbitMQ.Client;
using System.Text;

Console.WriteLine("=== RabbitMQ Producer - Fanout Exchange (Publish/Subscribe) ===");

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

// ExchangeDeclare is now ExchangeDeclareAsync
await channel.ExchangeDeclareAsync(exchange: exchangeName,
                                   type: ExchangeType.Fanout,
                                   durable: true);

Console.WriteLine($"Fanout Exchange created: {exchangeName}");

int messageCount = 0;

while (true)
{
    Console.WriteLine("\nEnter a message (or 'exit' to quit):");
    string? input = Console.ReadLine();

    if (input?.ToLower() == "exit") break;
    if (string.IsNullOrWhiteSpace(input)) continue;

    messageCount++;
    string message = $"Order Event #{messageCount}: {input} - Time: {DateTime.Now:HH:mm:ss}";
    var body = Encoding.UTF8.GetBytes(message);

    // BasicPublish is now BasicPublishAsync
    await channel.BasicPublishAsync(exchange: exchangeName,
                                    routingKey: string.Empty, // Ignored in Fanout exchanges
                                    body: body);

    Console.WriteLine($" [x] Message broadcast to all subscribers: {message}");
}

Console.WriteLine("Producer stopped.");

using RabbitMQ.Client;
using System.Text;

Console.WriteLine("=== RabbitMQ Producer - Topic Exchange (Advanced Routing) ===");

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

// ExchangeDeclare is now ExchangeDeclareAsync
await channel.ExchangeDeclareAsync(exchange: exchangeName,
                                   type: ExchangeType.Topic,
                                   durable: true);

Console.WriteLine($"Topic Exchange created: {exchangeName}");

var routingKeys = new[]
{
    "order.created",
    "order.payment.success",
    "order.payment.failed",
    "order.shipped",
    "order.delivered",
    "order.cancelled"
};

int messageId = 0;

while (true)
{
    Console.WriteLine("\nSelect a Routing Key (or type 'exit' to quit):");
    Console.WriteLine("1. order.created");
    Console.WriteLine("2. order.payment.success");
    Console.WriteLine("3. order.payment.failed");
    Console.WriteLine("4. order.shipped");
    Console.WriteLine("5. order.delivered");
    Console.WriteLine("6. order.cancelled");
    Console.Write("Enter your choice: ");

    string? input = Console.ReadLine();
    if (input?.ToLower() == "exit") break;

    if (!int.TryParse(input, out int choice) || choice < 1 || choice > 6)
    {
        Console.WriteLine("Invalid selection!");
        continue;
    }

    string routingKey = routingKeys[choice - 1];
    messageId++;

    string message = $"Event #{messageId} - {routingKey} - Time: {DateTime.Now:HH:mm:ss}";
    var body = Encoding.UTF8.GetBytes(message);

    // BasicPublish is now BasicPublishAsync
    await channel.BasicPublishAsync(exchange: exchangeName,
                                    routingKey: routingKey,
                                    body: body);

    Console.WriteLine($" [x] Message sent with Routing Key '{routingKey}'.");
}

Console.WriteLine("Producer stopped.");


using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Text;
using System.Threading;

Console.WriteLine("=== RabbitMQ Producer - Publisher Confirms ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// Use async methods and 'await using' for proper disposal
await using var connection = await factory.CreateConnectionAsync();

// In v7, Publisher Confirms are configured declaratively using CreateChannelOptions
var channelOptions = new CreateChannelOptions(
    publisherConfirmationsEnabled: true,
    publisherConfirmationTrackingEnabled: true
);

await using var channel = await connection.CreateChannelAsync(channelOptions);

const string exchangeName = "order.confirm.exchange";
const string queueName = "order.confirm.queue";

// Topology setups are now fully async
await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Direct, durable: true);
await channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: "order.created");

Console.WriteLine("Publisher Confirms enabled.");

int messageId = 0;

while (true)
{
    Console.WriteLine("\nEnter a message (or 'exit' to quit):");
    string? input = Console.ReadLine();

    if (input?.ToLower() == "exit") break;
    if (string.IsNullOrWhiteSpace(input)) continue;

    messageId++;
    string message = $"Order #{messageId}: {input} - Time: {DateTime.Now:HH:mm:ss}";
    var body = Encoding.UTF8.GetBytes(message);

    // Set a 5-second confirmation timeout using a CancellationToken
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

    try
    {
        // In v7, awaiting BasicPublishAsync automatically waits for the broker's Ack response
        await channel.BasicPublishAsync(exchange: exchangeName,
                                        routingKey: "order.created",
                                        body: body,
                                        cancellationToken: cts.Token);

        Console.WriteLine($" [✓] Message #{messageId} was successfully confirmed (Delivered to RabbitMQ)");
    }
    catch (PublishException)
    {
        // Caught if the broker explicitly rejects (Nacks) or returns the message
        Console.WriteLine($" [✗] Message #{messageId} was not confirmed! Please resend it.");
    }
    catch (OperationCanceledException)
    {
        // Caught if the 5-second timeout expires before the broker answers
        Console.WriteLine($" [✗] Message #{messageId} confirmation timed out!");
    }
}

Console.WriteLine("Producer stopped.");
*/

using RabbitMQ.Client;
using System.Text;

Console.WriteLine("=== RabbitMQ Producer - Consumer Acknowledgement Demo ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// Use async methods and 'await using' for proper disposal
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string exchangeName = "order.ack.exchange";
const string queueName = "order.ack.queue";

// Topology setups are now fully async
await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Direct, durable: true);
await channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);
await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: "order.process");

Console.WriteLine("Exchange and Queue are ready.");

int messageId = 0;

while (true)
{
    Console.WriteLine("\nEnter the order details (or 'exit' to quit):");
    string? input = Console.ReadLine();

    if (input?.ToLower() == "exit") break;
    if (string.IsNullOrWhiteSpace(input)) continue;

    messageId++;
    string message = $"Order #{messageId}: {input} - Time: {DateTime.Now:HH:mm:ss}";
    var body = Encoding.UTF8.GetBytes(message);

    // BasicPublish is now BasicPublishAsync
    await channel.BasicPublishAsync(exchange: exchangeName,
                                    routingKey: "order.process",
                                    body: body);

    Console.WriteLine($" [x] Order #{messageId} sent.");
}

Console.WriteLine("Producer stopped.");