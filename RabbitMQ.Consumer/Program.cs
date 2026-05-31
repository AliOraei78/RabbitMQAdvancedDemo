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
*/
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