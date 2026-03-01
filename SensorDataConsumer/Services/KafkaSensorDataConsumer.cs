using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SensorDataConsumer.Background;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services;

public sealed class KafkaSensorDataConsumer : IDisposable
{
    private readonly IConsumer<string, SensorData> _consumer;
    private readonly ILogger<KafkaSensorDataConsumer> _logger;
    private readonly KafkaOptions _options;

    public KafkaSensorDataConsumer(
        IOptions<KafkaOptions> options,
        ILogger<KafkaSensorDataConsumer> logger)
    {
        _options = options.Value;
        _logger = logger;

        _consumer = new ConsumerBuilder<string, SensorData>(new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = _options.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            })
            .SetValueDeserializer(new SystemTextJsonSerializer())
            .Build();

        _consumer.Subscribe(_options.Topic);

        _logger.LogInformation(
            "Kafka consumer subscribed to topic {Topic}, group {GroupId}",
            _options.Topic, _options.GroupId);
    }

    public List<SensorData> ConsumeBatch(CancellationToken ct)
    {
        var batch = new List<SensorData>(_options.MaxBatchSize);
        var deadline = DateTime.UtcNow + _options.FlushInterval;

        while (batch.Count < _options.MaxBatchSize && !ct.IsCancellationRequested)
        {
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                break;
            }

            try
            {
                var result = _consumer.Consume(remaining);
                if (result is null)
                {
                    break;
                }

                batch.Add(result.Message.Value);
            }
            catch (ConsumeException ex)
            {
                _logger.LogWarning(ex,
                    "Malformed message skipped at offset {Offset}",
                    ex.ConsumerRecord?.Offset);
            }
        }

        return batch;
    }

    public void Commit() => _consumer.Commit();

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
    }
}