using Microsoft.Extensions.Logging;

namespace SensorDataConsumer.Services;

/// <summary>
/// Запускает Producer и Consumer как две независимые задачи.
/// Producer пишет в буфер непрерывно.
/// Consumer читает по сигналу таймера и пишет в БД.
/// </summary>
public class SensorProcessor
{
    private readonly SensorDataProducer _producer;
    private readonly SensorDataConsumer _consumer;
    private readonly SensorProcessorOptions _options;
    private readonly ILogger<SensorProcessor> _logger;

    public SensorProcessor(SensorDataProducer producer, SensorDataConsumer consumer, SensorProcessorOptions options,
        ILogger<SensorProcessor> logger)
    {
        _producer = producer;
        _consumer = consumer;
        _options = options;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting sensor processor.");

        var buffer = new SensorDataBuffer(_options);
        var produceTask = _producer.ProduceAsync(buffer, cancellationToken);
        var consumeTask = _consumer.ConsumeAsync(buffer, cancellationToken);

        // Ждём завершения обеих задач.
        // При исключении в любой из них — оно пробросится наверх.
        await Task.WhenAll(produceTask, consumeTask);

        _logger.LogInformation("Sensor processor stopped.");
    }
}