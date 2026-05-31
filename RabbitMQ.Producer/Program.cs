using RabbitMQ.Client;
using System.Text;

Console.WriteLine("=== RabbitMQ Producer Started ===");

var factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// Use async methods and 'await using' for proper disposal
await using var connection = await factory.CreateConnectionAsync();

// CreateModel() is now CreateChannelAsync()
await using var channel = await connection.CreateChannelAsync();

const string queueName = "hello.queue";

// QueueDeclare is now QueueDeclareAsync
await channel.QueueDeclareAsync(queue: queueName,
                                durable: false,
                                exclusive: false,
                                autoDelete: false,
                                arguments: null);

string message = "Hello! This is the first message from the Producer. - " + DateTime.Now.ToString("HH:mm:ss");
var body = Encoding.UTF8.GetBytes(message);

// BasicPublish is now BasicPublishAsync
await channel.BasicPublishAsync(exchange: string.Empty,
                                routingKey: queueName,
                                body: body);

Console.WriteLine($" [x] Message sent: {message}");
Console.WriteLine("Press any key to exit...");
Console.ReadKey();