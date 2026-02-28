using Microsoft.Extensions.Logging;
using SensorDataConsumer.Abstractions;

namespace SensorDataConsumer.Services;

public class SensorDataProducer
{
    private readonly IMessageSource _source;
    private readonly SensorDataQueue _dataQueue;
    private readonly ILogger<SensorDataProducer> _logger;

    public SensorDataProducer(IMessageSource source, SensorDataQueue dataQueue, ILogger<SensorDataProducer> logger)
    {
        _source = source;
        _dataQueue = dataQueue;
        _logger = logger;
    }

    public async Task ProduceAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                var message = await _source.ReceiveAsync(cancellationToken);
                if (message is null)
                {
                    // Source исчерпан — сигнализируем consumer что новых сообщений не будет.
                    _dataQueue.Complete();
                    return;
                }

                await _dataQueue.Writer.WriteAsync(message, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            _dataQueue.Complete();
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Producer failed unexpectedly.");
            _dataQueue.Complete(ex);
            throw;
        }
    }
}