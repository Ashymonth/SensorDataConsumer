using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SensorDataConsumer.Services;

namespace SensorDataConsumer.Background;

public class SensorProcessorBackgroundService : BackgroundService
{
    private readonly SensorDataProducer _producer;
    private readonly Services.SensorDataConsumer _consumer;
    private readonly SensorDataQueue _queue;
    private readonly ILogger<SensorProcessorBackgroundService> _logger;

    public SensorProcessorBackgroundService(
        SensorDataProducer producer,
        Services.SensorDataConsumer consumer,
        SensorDataQueue queue,
        ILogger<SensorProcessorBackgroundService> logger)
    {
        _producer = producer;
        _consumer = consumer;
        _queue = queue;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Sensor processor starting");

        var produceTask = RunProducerAsync(stoppingToken);
        var consumeTask = RunConsumerAsync(stoppingToken);

        await Task.WhenAll(produceTask, consumeTask);

        _logger.LogInformation("Sensor processor stopped");
    }

    private async Task RunProducerAsync(CancellationToken ct)
    {
        try
        {
            await _producer.ProduceAsync(ct);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Producer cancelled");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Producer failed");
            throw;
        }
        finally
        {
            _queue.Writer.TryComplete();
        }
    }

    private async Task RunConsumerAsync(CancellationToken ct)
    {
        try
        {
            await _consumer.ConsumeAsync(ct);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer cancelled");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Consumer failed");
            throw;
        }
    }
}