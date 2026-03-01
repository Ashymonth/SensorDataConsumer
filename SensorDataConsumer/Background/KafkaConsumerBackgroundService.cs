using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SensorDataConsumer.Abstractions;
using SensorDataConsumer.Models;
using SensorDataConsumer.Services;

namespace SensorDataConsumer.Background;

public class KafkaConsumerBackgroundService : BackgroundService
{
    private readonly KafkaSensorDataConsumer _kafkaConsumer;
    private readonly IDataDestination _destination;
    private readonly ILogger<KafkaConsumerBackgroundService> _logger;

    public KafkaConsumerBackgroundService(KafkaSensorDataConsumer kafkaConsumer, IDataDestination destination,
        ILogger<KafkaConsumerBackgroundService> logger)
    {
        _kafkaConsumer = kafkaConsumer;
        _destination = destination;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var batch = _kafkaConsumer.ConsumeBatch(stoppingToken);

                if (batch.Count == 0)
                {
                    continue;
                }

                await ProcessBatchAsync(batch, stoppingToken);

                _kafkaConsumer.Commit();
            }
        }
        finally
        {
            _logger.LogInformation("Closing Kafka consumer");
        }
    }

    private async Task ProcessBatchAsync(List<SensorData> batch, CancellationToken ct)
    {
        _logger.LogInformation("Processing batch of {Count} messages in Kafka consumer", batch.Count);

        var latestBySensor = batch.GroupBy(d => d.SensorId).Select(g => g.MaxBy(d => d.Timestamp)!);

        try
        {
            await _destination.WriteBatchAsync(latestBySensor, ct);
            _logger.LogDebug("Batch processed successfully");
            return;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (DataValidationException ex)
        {
            _logger.LogWarning(ex, "Batch-level validation failed, falling back to per-sensor");
        }
        catch (DbUpdateException ex)
        {
            _logger.LogError(ex, "Database error on batch → falling back");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error on batch");
        }

        await WriteLatestPerSensorAsync(batch, ct);
    }

    private async Task WriteLatestPerSensorAsync(List<SensorData> batch, CancellationToken ct)
    {
        foreach (var group in batch.GroupBy(d => d.SensorId))
        {
            var sensorId = group.Key;
            var sensorSaved = false;

            foreach (var data in group.OrderByDescending(d => d.Timestamp))
            {
                if (sensorSaved)
                {
                    break;
                }

                try
                {
                    await _destination.WriteBatchAsync([data], ct);
                    sensorSaved = true;
                    _logger.LogDebug("Saved sensor {SensorId}", sensorId);
                }
                catch (DataValidationException ex)
                {
                    _logger.LogWarning(ex, "Validation failed for sensor {SensorId} dropping message", sensorId);
                }
                catch (DbUpdateException ex)
                {
                    _logger.LogError(ex, "DB error for sensor {SensorId} dropping after retries", sensorId);
                    if (await RetryWriteAsync(data, ct))
                    {
                        sensorSaved = true;
                    }
                }
                catch (Exception ex)
                {
                    // можно так же вынести в dlq
                    _logger.LogError(ex, "Unexpected error for sensor {SensorId} dropping", sensorId);
                }
            }
        }
    }

    private async Task<bool> RetryWriteAsync(SensorData data, CancellationToken ct)
    {
        // вынести в конфиг
        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(attempt), ct);
                await _destination.WriteBatchAsync([data], ct);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Retry {Attempt}/3 failed for sensor {SensorId}", attempt, data.SensorId);
            }
        }

        return false;
    }
}