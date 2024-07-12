using RabbitConsume;
using RabbitMQ.Client;
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

model.ConfirmSelect();

var exchangeName = "hello_exchange";
var queueName = "queue_hello";

model.ExchangeDeclare(exchangeName, "fanout", true, false, null);
model.QueueDeclare(queueName, true, false, false, null);
model.QueueBind(queueName, exchangeName, string.Empty, null);

var objMessage = new CustomObject() { Text = "Helooo" };
var message = JsonSerializer.Serialize(objMessage);
var body = Encoding.UTF8.GetBytes(message);

var propsBasics = model.CreateBasicProperties();
propsBasics.Headers = new Dictionary<string, object>() {{ "content-type", "application/json" }};
propsBasics.DeliveryMode = 2;

for (int i = 0; i < 100_000_000; i++)
    model.BasicPublish(exchangeName, string.Empty, propsBasics, body);

Console.ReadLine();