using Microsoft.Extensions.Logging;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services;

public class SensorDataDeduplicator
{
    private readonly ILogger<SensorDataDeduplicator> _logger;

    public SensorDataDeduplicator(ILogger<SensorDataDeduplicator> logger)
    {
        _logger = logger;
    }

    public (List<Message<SensorData>> toWrite, List<Message<SensorData>> toDiscard)
        Deduplicate(List<Message<SensorData>> batch)
    {
        var latestBySensor = new Dictionary<string, Message<SensorData>>(batch.Count);

        foreach (var message in batch)
        {
            var id = message.Data.SensorId;
            if (!latestBySensor.TryGetValue(id, out var existing) ||
                message.Data.Timestamp > existing.Data.Timestamp)
            {
                if (existing is not null)
                {
                    _logger.LogDebug(
                        "Deduplicating message for sensor {SensorId}. Previous time: {PreviousTime}. Current time: {CurrentTime}",
                        id, existing.Data.Timestamp, message.Data.Timestamp);
                }

                latestBySensor[id] = message;
            }
        }

        var toWrite = new List<Message<SensorData>>(latestBySensor.Count);
        var toDiscard = new List<Message<SensorData>>(batch.Count - latestBySensor.Count);

        foreach (var message in batch)
        {
            if (latestBySensor.TryGetValue(message.Data.SensorId, out var latest) && ReferenceEquals(latest, message))
            {
                toWrite.Add(message);
            }
            else
            {
                toDiscard.Add(message);
            }
        }

        return (toWrite, toDiscard);
    }
}