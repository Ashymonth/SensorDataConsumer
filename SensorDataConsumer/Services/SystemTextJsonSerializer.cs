using System.Text.Json;
using Confluent.Kafka;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services;

public class SystemTextJsonSerializer : IDeserializer<SensorData>
{
    public SensorData Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
        {
            return null!;
        }

        return JsonSerializer.Deserialize<SensorData>(data)!;
    }
}