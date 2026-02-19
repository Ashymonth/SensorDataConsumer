using SensorDataConsumer.Abstractions;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services;

public class FakeDataValidator : IDataValidator
{
    public bool IsMessageValid(Message<SensorData> message)
    {
        return message.Data.SensorId != "123";
    }
}