namespace SensorDataConsumer;

public class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    
    public string Topic { get; set; } = "sensor-data";
    
    public string GroupId { get; set; } = "SensorDataConsumer";
    
    public int MaxBatchSize { get; set; } = 100;
    
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(5);
}