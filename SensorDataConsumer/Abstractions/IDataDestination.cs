using SensorDataConsumer.Models;

namespace SensorDataConsumer.Abstractions;

public interface IDataDestination
{
    /// <exception cref="DbUpdateException" />
    Task WriteBatchAsync(IEnumerable<SensorData> payload, CancellationToken cancellationToken);
}

