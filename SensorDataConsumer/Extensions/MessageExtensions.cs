using Microsoft.Extensions.Logging;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Extensions;

public static class MessageExtensions
{
    extension(IEnumerable<Message<SensorData>> messages)
    {
        /// <summary>
        /// Подтверждает все сообщения. Исключения логируются, но не прерывают обработку.
        /// </summary>
        public async Task AckAllAsync(ILogger logger)
        {
            foreach (var message in messages)
            {
                try
                {
                    await message.AckAsync();
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex,
                        "Failed to Ack message for sensor {SensorId}. " +
                        "Message may be redelivered.",
                        message.Data.SensorId);
                }
            }
        }

        /// <summary>
        /// Отклоняет все сообщения. Исключения логируются, но не прерывают обработку.
        /// </summary>
        public async Task NackAllAsync(bool requeue, ILogger logger)
        {
            foreach (var message in messages)
            {
                try
                {
                    await message.NackAsync(requeue);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex,
                        "Failed to Nack message for sensor {SensorId}. " +
                        "Message may be redelivered regardless.",
                        message.Data.SensorId);
                }
            }
        }
    }
}