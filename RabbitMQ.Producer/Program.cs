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
*/

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