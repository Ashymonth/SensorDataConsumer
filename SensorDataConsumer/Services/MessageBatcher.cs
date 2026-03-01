using System.Runtime.CompilerServices;
using System.Threading.Channels;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services;

public sealed class MessageBatcher
{
    private readonly int _maxBatchSize;
    private readonly TimeSpan _flushInterval;
    private readonly SensorDataQueue _dataQueue;

    public MessageBatcher(KafkaOptions options, SensorDataQueue dataQueue)
    {
        _maxBatchSize = options.MaxBatchSize;
        _flushInterval = options.FlushInterval;
        _dataQueue = dataQueue;
    }

    /// <summary>
    /// Возвращает асинхронную последовательность батчей.
    /// При graceful shutdown (отмена ct) отдаёт остатки перед завершением.
    /// </summary>
    public async IAsyncEnumerable<IReadOnlyList<Message<SensorData>>> CreateBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var reader = _dataQueue.Reader;
        var batch = new List<Message<SensorData>>(_maxBatchSize);

        // Читаем батчи пока канал открыт
        // ReadBatchesAsync ловит OperationCanceledException от ct
        // и завершается штатно, чтобы мы могли сделать финальный drain
        await foreach (var readyBatch in ReadBatchesAsync(reader, batch, ct))
        {
            yield return readyBatch;
            batch = new List<Message<SensorData>>(_maxBatchSize);
        }

        // Канал закрыт, или ct отменён — забираем остатки
        FillBatch(reader, batch);

        if (batch.Count > 0)
        {
            yield return batch;
        }
    }
    
    private async IAsyncEnumerable<List<Message<SensorData>>> ReadBatchesAsync(
        ChannelReader<Message<SensorData>> reader,
        List<Message<SensorData>> batch,
        [EnumeratorCancellation] CancellationToken externalCancellationToken)
    {
        while (!externalCancellationToken.IsCancellationRequested)
        {
            bool hasData;

            try
            {
                hasData = await reader.WaitToReadAsync(externalCancellationToken);
            }
            catch (OperationCanceledException) 
            {
                break; // graceful shutdown - выходим для финального drain
            }

            if (!hasData)
            {
                break;  
            }

            FillBatch(reader, batch);

            if (batch.Count < _maxBatchSize)
            {
                try
                {
                    await WaitForFullBatchOrTimeoutAsync(reader, batch, externalCancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            if (batch.Count > 0)
            {
                yield return batch;
            }
        }
        
        // финальный вызов после закрытия, если что-то осталось
        if (batch.Count > 0)
        {
            yield return batch;
        }
    }

    private async Task WaitForFullBatchOrTimeoutAsync(
        ChannelReader<Message<SensorData>> reader,
        List<Message<SensorData>> batch,
        CancellationToken externalCancellationToken)
    {
        if (_flushInterval <= TimeSpan.Zero || batch.Count == 0)
        {
            return;
        }

        using var ctsTimeout = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken);
        ctsTimeout.CancelAfter(_flushInterval);

        try
        {
            while (batch.Count < _maxBatchSize && await reader.WaitToReadAsync(ctsTimeout.Token))
            {
                FillBatch(reader, batch);
            }
        }
        catch (OperationCanceledException) when (!externalCancellationToken.IsCancellationRequested)
        {
            // flushInterval истёк - штатный flush по таймеру
        }
    }

    private void FillBatch(ChannelReader<Message<SensorData>> reader, List<Message<SensorData>> batch)
    {
        while (batch.Count < _maxBatchSize && reader.TryRead(out var item))
        {
            batch.Add(item);
        }
    }
}