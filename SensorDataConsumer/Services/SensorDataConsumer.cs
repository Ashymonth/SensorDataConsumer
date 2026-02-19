using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using SensorDataConsumer.Abstractions;
using SensorDataConsumer.Extensions;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services;

public class SensorDataConsumer
{
    private readonly SensorDataDeduplicator _deduplicator;
    private readonly SensorProcessorOptions _options;
    private readonly IDataDestination _destination;
    private readonly ILogger<SensorDataConsumer> _logger;

    public SensorDataConsumer(SensorDataDeduplicator deduplicator,
        SensorProcessorOptions options,
        IDataDestination destination,
        ILogger<SensorDataConsumer> logger)
    {
        _deduplicator = deduplicator;
        _options = options;
        _destination = destination;
        _logger = logger;
    }

    public async Task ConsumeAsync(SensorDataBuffer buffer, CancellationToken cancellationToken)
    {
        try
        {
            while (await buffer.WaitForNextTickAsync(cancellationToken))
            {
                await DrainBatchAndProcessAsync(buffer, cancellationToken);

                if (!buffer.IsFinished)
                {
                    continue;
                }
                // Source завершил работу — DrainBatchAndProcessAsync уже всё вычитал,
                // но между последним Drain и этой проверкой producer мог дописать.
                // Делаем финальный DrainBatchAndProcessAsync на всякий случай.
                await DrainBatchAndProcessAsync(buffer, cancellationToken);
                return;
            }
        }
        catch (OperationCanceledException)
        {
            await DrainBatchAndProcessAsync(buffer, CancellationToken.None);
            throw;
        }
    }

    private async Task DrainBatchAndProcessAsync(SensorDataBuffer dataBuffer, CancellationToken cancellationToken)
    {
        var batch = dataBuffer.Drain(_options.MaxBatchSize);
        await ProcessBatchAsync(batch, cancellationToken);
    }

    private async Task ProcessBatchAsync(List<Message<SensorData>> batch, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Processing batch of: {Size}", batch.Count);

        if (batch.Count == 0)
        {
            return;
        }

        var (toWrite, toDiscard) = _deduplicator.Deduplicate(batch);

        await toDiscard.AckAllAsync(_logger);

        try
        {
            // Ошибка DataValidationException сюда не попадет, тк продюсер ее не пропустит. Поэтому проверяем только на 1 ошибку 
            await _destination.WriteBatchAsync(toWrite.Select(m => m.Data), cancellationToken);
            await toWrite.AckAllAsync(_logger);
        }
        catch (DbUpdateException updateException)
        {
            _logger.LogError(updateException, "Failed to write batch of {Count} messages. Requesting.", toWrite.Count);

            // Либо можно рекурсивно сохранить только валидные сообщения
            var (failed, healthy) = IsolateFailedEntries(updateException, toWrite);

            // Исходим из того, что это ошибка указывает на то, что есть запись с более поздней датой.
            // Если есть другие ошибки, то нужно проверять на коды или использовать более явные ошибки
            await failed.NackAllAsync(requeue: false, _logger);
            await healthy.NackAllAsync(requeue: true,
                _logger); // Повторно попробуем их сохранить потом, если они все еще будут актуальны
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write batch of {Count} messages. Requesting.", toWrite.Count);

            await toWrite.NackAllAsync(requeue: false, _logger); // Нет смысла пытаться падать снова и снова, когда эти данные повторно прилетят
        }
    }

    private (List<Message<SensorData>> failed, List<Message<SensorData>> healthy)
        IsolateFailedEntries(DbUpdateException ex, List<Message<SensorData>> messages)
    {
        var failedIds = ex.Entries
            .Select(e => e.Entity)
            .OfType<SensorData>()
            .Select(s => s.SensorId)
            .ToHashSet();

        if (failedIds.Count == 0)
        {
            // Не можем изолировать — считаем весь батч проблемным
            _logger.LogWarning("DbUpdateException has no Entries, cannot isolate failed messages.");
            return (messages, []);
        }

        // к этому моменту мы уже сделали дедупликацию и у нас нет сообщений с несколькими sensorId
        var failed = messages.Where(message => failedIds.Contains(message.Data.SensorId)).ToList();
        var healthy = messages.Where(message => !failedIds.Contains(message.Data.SensorId)).ToList();

        _logger.LogWarning("Isolated {FailedCount} failed entries out of {Total}.",
            failed.Count, messages.Count);

        return (failed, healthy);
    }
}