using System.Threading.Channels;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services;

/// <summary>
/// Хранит сообщения от Producer и сигнализирует Consumer когда пора читать.
/// </summary>
public sealed class SensorDataBuffer : IDisposable
{
    private readonly Channel<Message<SensorData>> _channel;
    private readonly PeriodicTimer _timer;

    public SensorDataBuffer(SensorProcessorOptions options)
    {
        // тут так же можем указать Bounded и прокинуть сюда BatchMaxSize
        _channel = Channel.CreateUnbounded<Message<SensorData>>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true
            });

        _timer = new PeriodicTimer(options.FlushInterval);
    }

    public bool IsFinished => _channel.Reader.Completion.IsCompleted;

    public ValueTask WriteAsync(Message<SensorData> message, CancellationToken cancellationToken) =>
        _channel.Writer.WriteAsync(message, cancellationToken);

    public void Complete(Exception? ex = null) => _channel.Writer.Complete(ex);

    /// <summary>
    /// Ждём следующего тика таймера.
    /// Возвращает false если таймер был disposed — consumer должен завершиться.
    /// </summary>
    public ValueTask<bool> WaitForNextTickAsync(CancellationToken cancellationToken)
        => _timer.WaitForNextTickAsync(cancellationToken);

    public List<Message<SensorData>> Drain(int maxBatchSize)
    {
        var batch = new List<Message<SensorData>>(maxBatchSize);
        while (batch.Count < maxBatchSize && _channel.Reader.TryRead(out var sensorData))
        {
            batch.Add(sensorData);
        }

        return batch;
    }

    public void Dispose()
    {
        _timer.Dispose();
    }
}