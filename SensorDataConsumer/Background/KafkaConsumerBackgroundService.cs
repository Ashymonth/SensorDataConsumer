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

                try
                {
                    await ProcessBatchAsync(batch, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Batch processing failed, committing to avoid reprocessing");
                }
                
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
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Batch write failed, falling back to per-sensor");
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
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (DataValidationException ex)
                {
                    _logger.LogWarning(ex,
                        "Validation failed for sensor {SensorId}, trying older value",
                        group.Key);
                    // пробуем следующее значение
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Write failed for sensor {SensorId}, retrying", group.Key);

                    if (await RetryWriteAsync(data, ct))
                    {
                        break;
                    }
                }
            }
        }
    }

    private async Task<bool> RetryWriteAsync(SensorData data, CancellationToken ct)
    {
        // значения брать из конфига
        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(attempt), ct);
                await _destination.WriteBatchAsync([data], ct);
                return true;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (DataValidationException ex)
            {
                _logger.LogWarning(ex, "Validation failed for sensor {SensorId} dropping message", data.SensorId);
                return false;

            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Retry {Attempt}/3 failed for sensor {SensorId}",
                    attempt, data.SensorId);
            }
        }

        return false;
    }
}