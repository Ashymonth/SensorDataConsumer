using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services; 

public class KafkaSensorDataProducer : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;
    private readonly ILogger<KafkaSensorDataProducer> _logger;

    public KafkaSensorDataProducer(IOptions<KafkaOptions> options, ILogger<KafkaSensorDataProducer> logger)
    {
        _logger = logger;
        _topic = options.Value.Topic;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            Partitioner = Partitioner.Murmur2
        }).Build();
    }


    /// <summary>
    /// Отправляет сообщение в Kafka. Бросает исключение при любой ошибке.
    /// </summary>
    public async Task ProduceAsync(string key, SensorData data, CancellationToken ct = default)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = JsonSerializer.Serialize(data)
        };

        try
        {
            await _producer.ProduceAsync(_topic, message, ct).ConfigureAwait(false);
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex, "Failed to produce message for sensor {SensorId}, key={Key}", data.SensorId, key);
            throw;
        }
    }

    public void Flush(TimeSpan timeout) => _producer.Flush(timeout);

    public void Dispose() => _producer.Dispose();
}