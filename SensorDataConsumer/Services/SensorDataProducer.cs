using Microsoft.Extensions.Logging;
using SensorDataConsumer.Abstractions;

namespace SensorDataConsumer.Services;

public class SensorDataProducer
{
    private readonly IMessageSource _source;
    private readonly IDataValidator _validator;
    private readonly ILogger<SensorDataProducer> _logger;

    public SensorDataProducer(IMessageSource source, IDataValidator validator, ILogger<SensorDataProducer> logger)
    {
        _source = source;
        _validator = validator;
        _logger = logger;
    }

    public async Task ProduceAsync(SensorDataBuffer dataBuffer, CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                var message = await _source.ReceiveAsync(cancellationToken);
                if (message is null)
                {
                    // Source исчерпан — сигнализируем consumer что новых сообщений не будет.
                    dataBuffer.Complete();
                    return;
                }

                if (!_validator.IsMessageValid(message))
                {
                    await message.NackAsync(false); // Повторять не нужно
                    continue;
                }

                // Можно выбирать разные стратегии при ожидании. Вплоть до установки лимита на сообщения в памяти
                await dataBuffer.WriteAsync(message, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            dataBuffer.Complete();
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Producer failed unexpectedly.");
            dataBuffer.Complete(ex);
            throw;
        }
    }
}