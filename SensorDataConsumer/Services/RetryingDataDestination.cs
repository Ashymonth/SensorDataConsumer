using SensorDataConsumer.Abstractions;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services;

public class RetryingDataDestination : IDataDestination
{
    public Task WriteBatchAsync(IEnumerable<SensorData> payload, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}