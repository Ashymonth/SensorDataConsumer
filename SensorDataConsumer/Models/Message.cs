namespace SensorDataConsumer.Models;

public class Message<T>
{
    public T Data { get; set; } = default!;

    public Task AckAsync()
    {
        return Task.CompletedTask;
    }

    public Task NackAsync(bool requeue)
    {
        return Task.CompletedTask;
    }
}