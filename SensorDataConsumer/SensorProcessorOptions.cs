namespace SensorDataConsumer;

public class SensorProcessorOptions
{
    public int MaxBatchSize { get; init; } = 100;

    /// <summary>Интервал таймера — как часто сбрасываем накопленное в БД.</summary>
    public TimeSpan FlushInterval { get; init; } = TimeSpan.FromMilliseconds(500);
}