using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SensorDataConsumer;
using SensorDataConsumer.Abstractions;
using SensorDataConsumer.Services;

var services = new ServiceCollection();

services.AddLogging(b => b.AddConsole().AddDebug());

services.AddSingleton<SensorProcessor>();
services.AddSingleton<SensorDataProducer>();
services.AddSingleton<MessageBatcher>();
services.AddSingleton<SensorDataQueue>();
services.AddSingleton<SensorDataConsumer.Services.SensorDataConsumer>();
services.AddSingleton<SensorProcessorOptions>();
services.AddSingleton<IDataDestination, RetryingDataDestination>();
services.AddSingleton<IMessageSource, FakeMessageSource>();

var provider = services.BuildServiceProvider();

var processor = provider.GetRequiredService<SensorProcessor>();

await processor.RunAsync(CancellationToken.None);