namespace SensorDataConsumer;

public class SensorDataTopicConfig
{
    public string TopicName { get; set; } = "sensor-data";
    
    public string BootstrapServers { get; set; } = "localhost:9092";
    
    public string GroupId { get; set; } = "SensorDataConsumer";

    public bool EnableAutoCommit { get; set; } = false;
}