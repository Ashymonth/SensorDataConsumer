using Microsoft.Extensions.Logging;
using SensorDataConsumer.Abstractions;
using SensorDataConsumer.Extensions;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services;

public class SensorDataConsumer
{
    private static readonly TimeSpan DelayAfterFailure = TimeSpan.FromSeconds(1);
    
    private readonly IDataDestination _destination;
    private readonly MessageBatcher _batcher;
    private readonly ILogger<SensorDataConsumer> _logger;

    public SensorDataConsumer(IDataDestination destination, MessageBatcher batcher, ILogger<SensorDataConsumer> logger)
    {
        _destination = destination;
        _batcher = batcher;
        _logger = logger;
    }

    public async Task ConsumeAsync(CancellationToken ct)
    {
        // можно так же вынести в конфиг
        var consecutiveFailures = 0;
        const int maxConsecutiveFailures = 3;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await foreach (var batch in _batcher.CreateBatchesAsync(ct))
                {
                    await ProcessBatchAsync(batch, ct);
                    consecutiveFailures = 0;
                }
 
                return; // канал закрыт штатно
            }
            catch (Exception ex)
            {
                consecutiveFailures++;
                
                _logger.LogError(ex, "Consumer failed ({Failures}/{Max})", consecutiveFailures, maxConsecutiveFailures);

                if (consecutiveFailures >= maxConsecutiveFailures)
                {
                    throw; 
                }

                await Task.Delay(DelayAfterFailure, ct);
            }
        }
    }
    
    private async Task ProcessBatchAsync(IReadOnlyCollection<Message<SensorData>> batch,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Processing batch of: {Size}", batch.Count);

        if (batch.Count == 0)
        {
            return;
        }

        var latestBySensor = batch
            .GroupBy(m => m.Data.SensorId)
            .Select(g => g.MaxBy(m => m.Data.Timestamp)!);

        try
        {
            await _destination.WriteBatchAsync(latestBySensor.Select(m => m.Data), cancellationToken);
            await batch.AckAllAsync(_logger);
        }
        catch (DataValidationException ex)
        {
            _logger.LogError(ex, "DataValidation failed for batch of {Count} messages", batch.Count);
            await WriteLatestPerSensorAsync(batch, cancellationToken);
        }
        catch (DbUpdateException ex)
        {
            _logger.LogError(ex, "DbUpdate failed for batch of {Count} messages", batch.Count);
            await WriteLatestPerSensorAsync(batch, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Unexpected error writing batch of {Count} messages", batch.Count);
            await WriteLatestPerSensorAsync(batch, cancellationToken);
        }
    }

    private async Task WriteLatestPerSensorAsync(IReadOnlyCollection<Message<SensorData>> messages,
        CancellationToken token)
    {
        // можно через Parallel обрабатывать для каждого сенсора
        foreach (var messageGroup in messages.GroupBy(message => message.Data.SensorId))
        {
            var hasAnySavedSensorData = false;
            
            foreach (var message in messageGroup.OrderByDescending(m => m.Data.Timestamp))
            {
                if (hasAnySavedSensorData)
                {
                    await message.AckSaveAsync(_logger);
                    continue;
                }

                try
                {
                    await _destination.WriteBatchAsync([message.Data], token);
                    await message.AckAsync();
                    hasAnySavedSensorData = true;
                }
                catch (DataValidationException)
                {
                    _logger.LogError("DataValidation failed. SensorId={SensorId}, Timestamp={Timestamp}",
                        message.Data.SensorId,
                        message.Data.Timestamp);
                    
                    await message.NackSaveAsync(false, _logger);
                }
                catch (DbUpdateException)
                {
                    _logger.LogError("DbUpdate failed for message: {Message}", message.Data);
                    
                    // в случае если можем получить точную причину ошибки в DbUpdateException, то можем по разному ack/nack делать,
                    // но в таком виде считаем, что может быть транзитная ошибка БД и ретраим сообщение
                    await message.NackSaveAsync(true, _logger);
                }
                catch
                {
                    _logger.LogError("Unexpected error writing message: {Message}", message.Data);
                    await message.NackSaveAsync(true, _logger);
                }
            }
        }
    }
}