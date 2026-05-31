using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks; // Required for Task.CompletedTask

Console.WriteLine("=== RabbitMQ Consumer Started ===");

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

await channel.QueueDeclareAsync(queue: queueName,
                     durable: false,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);

Console.WriteLine(" [*] Waiting for messages...");

// Use AsyncEventingBasicConsumer for v7
var consumer = new AsyncEventingBasicConsumer(channel);

// Received is now ReceivedAsync and expects a Task return
consumer.ReceivedAsync += (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [✓] Message received: {message}");

    // Return Task.CompletedTask because we are not using 'await' inside this lambda
    return Task.CompletedTask;
};

// BasicConsume is now BasicConsumeAsync
await channel.BasicConsumeAsync(queue: queueName,
                     autoAck: true,
                     consumer: consumer);

Console.WriteLine("Press any key to exit...");
Console.ReadKey();