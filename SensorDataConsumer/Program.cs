using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SensorDataConsumer;
using SensorDataConsumer.Abstractions;
using SensorDataConsumer.Services;

var services = new ServiceCollection();

services.AddLogging(b => b.AddConsole().AddDebug());

services.AddSingleton<SensorProcessor>();
services.AddSingleton<SensorDataProducer>();
services.AddSingleton<SensorDataDeduplicator>();
services.AddSingleton<SensorDataConsumer.Services.SensorDataConsumer>();
services.AddSingleton<SensorProcessorOptions>();
services.AddSingleton<IDataDestination, RetryingDataDestination>();
services.AddSingleton<IMessageSource, FakeMessageSource>();
services.AddSingleton<IDataValidator, FakeDataValidator>();

var provider = services.BuildServiceProvider();

var processor = provider.GetRequiredService<SensorProcessor>();

await processor.RunAsync(CancellationToken.None);