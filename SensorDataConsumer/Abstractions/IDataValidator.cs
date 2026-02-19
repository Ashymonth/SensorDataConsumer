using SensorDataConsumer.Models;

namespace SensorDataConsumer.Abstractions;

/// <summary>
/// Проверяет сообщение на валидационные ошибки.
/// Уровень валидации не должен доходить до отвественности сервиса, который просто сохраняет, как мне кажется. Вынес отдельно
/// </summary>
public interface IDataValidator
{
    bool IsMessageValid(Message<SensorData> message);
}