using SensorDataConsumer.Models;

namespace SensorDataConsumer.Abstractions;

public interface IMessageSource
{
    /// <remarks>
    /// Если в данный момент сообщений нет, метод не завершает выполнение сразу,
    /// а асинхронно ожидает появления нового сообщения.
    /// Возвращает null, если источник завершил работу и новых сообщений больше не ожидается.
    /// </remarks>    
    Task<Message<SensorData>?> ReceiveAsync(CancellationToken cancellationToken);
}