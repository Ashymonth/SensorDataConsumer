using System.Threading.Channels;
using SensorDataConsumer.Models;

namespace SensorDataConsumer.Services;

/// <summary>
/// Хранит сообщения от Producer и сигнализирует Consumer когда пора читать.
/// </summary>
public sealed class SensorDataQueue
{
    private readonly Channel<Message<SensorData>> _channel;

    public SensorDataQueue(KafkaOptions options)
    {
        _channel = Channel.CreateBounded<Message<SensorData>>(
            new BoundedChannelOptions(options.MaxBatchSize)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.DropOldest
            });
    }

    public ChannelReader<Message<SensorData>> Reader => _channel.Reader;

    public ChannelWriter<Message<SensorData>> Writer => _channel.Writer;
}