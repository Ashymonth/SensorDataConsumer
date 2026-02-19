using Bogus;
using SensorDataConsumer.Abstractions;
using SensorDataConsumer.Models;

namespace SensorDataConsumer;

public class FakeMessageSource : IMessageSource
{
    private static readonly Faker _faker = new();
    private static readonly PeriodicTimer _periodicTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(1));

    private static readonly string[] _sensorIds = ["1", "2", "3", "4", "5", "6"];

    public async Task<Message<SensorData>?> ReceiveAsync(CancellationToken cancellationToken)
    {
        while (await _periodicTimer.WaitForNextTickAsync(cancellationToken))
        {
            _periodicTimer.Period = TimeSpan.FromMilliseconds(Random.Shared.Next(1, 10)); // Неравномерная скорость
            return new Message<SensorData>
            {
                Data = new SensorData(_faker.PickRandom(_sensorIds), _faker.Random.Double(1, 100), _faker.Date.Future())
            };
        }

        return null;
    }
}