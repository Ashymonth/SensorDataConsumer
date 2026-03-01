using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SensorDataConsumer.Abstractions;
using SensorDataConsumer.Extensions;
using SensorDataConsumer.Services;

namespace SensorDataConsumer.Background;

public class KafkaProducerBackgroundService : BackgroundService
{
    private readonly IMessageSource _messageSource;
    private readonly KafkaSensorDataProducer _kafkaProducer;
    private readonly ILogger<KafkaProducerBackgroundService> _logger;

    public KafkaProducerBackgroundService(
        IMessageSource messageSource,
        KafkaSensorDataProducer kafkaProducer,
        ILogger<KafkaProducerBackgroundService> logger)
    {
        _messageSource = messageSource;
        _kafkaProducer = kafkaProducer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var externalMessage = await _messageSource.ReceiveAsync(stoppingToken);
                if (externalMessage is null)
                {
                    _logger.LogInformation("Message source exhausted, stopping producer");
                    break;
                }

                try
                {
                    await _kafkaProducer.ProduceAsync(externalMessage.Data.SensorId, externalMessage.Data,
                        stoppingToken);

                    await externalMessage.AckSaveAsync(_logger);
                }
                catch (ProduceException<string, string> ex)
                {
                    _logger.LogError(ex, "Failed to produce message for sensor {SensorId}",
                        externalMessage.Data.SensorId);

                    await externalMessage.NackSaveAsync(true, _logger);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex, "Unexpected error while producing message for sensor {SensorId}",
                        externalMessage.Data.SensorId);

                    await externalMessage.NackSaveAsync(true, _logger);
                }
            }
        }
        finally
        {
            _logger.LogInformation("Flushing Kafka producer before shutdown...");
            _kafkaProducer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}