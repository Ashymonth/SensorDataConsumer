using Confluent.Kafka;
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
        while (!stoppingToken.IsCancellationRequested)
        {
            var batch = _kafkaConsumer.ConsumeBatch(stoppingToken);
            if (batch.Count == 0)
            {
                continue;
            }

            // словарь для быстрого поиска offset по данным
            var offsetMap = batch.ToDictionary(r => r.Message.Value, r => r);

            try
            {
                var messages = batch.Select(r => r.Message.Value).ToList();
                await ProcessBatchAsync(messages, offsetMap, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Batch processing failed, committing to avoid reprocessing");
            }

            _kafkaConsumer.Commit(batch);
        }
    }

    private async Task ProcessBatchAsync(
        List<SensorData> batch,
        Dictionary<SensorData, ConsumeResult<string, SensorData>> offsetMap,
        CancellationToken ct)
    {
        var latestBySensor = batch.GroupBy(d => d.SensorId).Select(g => g.MaxBy(d => d.Timestamp)!);

        try
        {
            await _destination.WriteBatchAsync(latestBySensor, ct);
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

        await WriteLatestPerSensorAsync(batch, offsetMap, ct);
    }

    private async Task WriteLatestPerSensorAsync(
        List<SensorData> batch,
        Dictionary<SensorData, ConsumeResult<string, SensorData>> offsetMap,
        CancellationToken ct)
    {
        foreach (var group in batch.GroupBy(d => d.SensorId))
        {
            foreach (var data in group.OrderByDescending(d => d.Timestamp))
            {
                try
                {
                    await _destination.WriteBatchAsync([data], ct);

                    if (offsetMap.TryGetValue(data, out var result))
                    {
                        _kafkaConsumer.Commit(result);
                    }

                    break;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (DataValidationException ex)
                {
                    _logger.LogWarning(ex, "Validation failed for {SensorId}, trying older", data.SensorId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Write failed for {SensorId}", data.SensorId);
                    if (await RetryWriteAsync(data, ct))
                        break;
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