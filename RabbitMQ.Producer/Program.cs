using RabbitMQ.Client;
using System.Text;

Console.WriteLine("=== RabbitMQ Producer - Direct Exchange ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// Use async methods and 'await using' for proper disposal
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

// Declare Direct Exchange
const string exchangeName = "order.direct.exchange";
const string queueName1 = "order.queue.payment";
const string queueName2 = "order.queue.shipping";

// Exchange, queue, and binding declarations are now async
await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Direct, durable: true);

await channel.QueueDeclareAsync(queue: queueName1, durable: true, exclusive: false, autoDelete: false);
await channel.QueueDeclareAsync(queue: queueName2, durable: true, exclusive: false, autoDelete: false);

await channel.QueueBindAsync(queue: queueName1, exchange: exchangeName, routingKey: "payment");
await channel.QueueBindAsync(queue: queueName2, exchange: exchangeName, routingKey: "shipping");

Console.WriteLine("Exchange and queues have been created successfully.");

while (true)
{
    Console.WriteLine("\nEnter order type (payment / shipping) or 'exit' to quit:");
    string? type = Console.ReadLine()?.ToLower();

    if (type == "exit") break;
    if (type != "payment" && type != "shipping")
    {
        Console.WriteLine("Only 'payment' or 'shipping' are allowed!");
        continue;
    }

    string message = $"New Order - Type: {type.ToUpper()} - Time: {DateTime.Now:HH:mm:ss}";
    var body = Encoding.UTF8.GetBytes(message);

    // BasicPublish is now BasicPublishAsync
    await channel.BasicPublishAsync(exchange: exchangeName,
                                    routingKey: type,
                                    body: body);

    Console.WriteLine($" [x] Message sent to {type.ToUpper()}: {message}");
}

Console.WriteLine("Producer stopped.");