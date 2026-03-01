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

    public async Task ProduceAsync(CancellationToken ct)
    {
        while (true)
        {
            var message = await _source.ReceiveAsync(ct);

            if (message is null)
            {
                _logger.LogInformation("Source exhausted, stopping producer");
                return;
            }

            await _dataQueue.Writer.WriteAsync(message, ct);
        }
    }
}