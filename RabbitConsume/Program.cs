using RabbitConsume;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

var connectionFactory = new ConnectionFactory()
{
    UserName = "username",
    Password = "password",
    VirtualHost = "test_host",
    Port = 5672,
    NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
    AutomaticRecoveryEnabled = true,
};

using var connection = connectionFactory.CreateConnection();
using var model = connection.CreateModel();

var exchangeName = "hello_exchange";
var queueName = "queue_hello";

model.ExchangeDeclare(exchangeName, "fanout", true, false, null);
model.QueueDeclare(queueName, true, false, false, null);
model.QueueBind(queueName, exchangeName, string.Empty, null);

model.BasicQos(0, 10, false);

var consumer = new EventingBasicConsumer(model);
consumer.Received += (innerModel, ea) =>
{
    var body = ea.Body;
    var message = Encoding.UTF8.GetString(body.ToArray());
    var objMessage = default(CustomObject);

    try
    {
        objMessage = JsonSerializer.Deserialize<CustomObject>(message);
    }
    catch(Exception)
    {
        model.BasicReject(ea.DeliveryTag, false);
        throw;
    }

    try
    {
        var service = new TestService();
        service.Execute(objMessage);

        model.BasicAck(ea.DeliveryTag, false);
    }
    catch (Exception)
    {
        model.BasicNack(ea.DeliveryTag, false, true);
        throw;
    }
};

model.BasicConsume(queueName, false, consumer);

Console.ReadLine();