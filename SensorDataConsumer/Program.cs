using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SensorDataConsumer;
using SensorDataConsumer.Abstractions;
using SensorDataConsumer.Background;
using SensorDataConsumer.Extensions;
using SensorDataConsumer.Services;

var builder = Host.CreateApplicationBuilder(args);
 
builder.Services.AddLogging(b => b.AddConsole().AddDebug());
 
builder.Services.AddSingleton<SensorDataProducer>();
builder.Services.AddSingleton<MessageBatcher>();
builder.Services.AddSingleton<SensorDataQueue>();
builder.Services.AddSingleton<SensorDataConsumer.Services.SensorDataConsumer>();
builder.Services.AddSingleton<KafkaOptions>();

builder.Services.AddSingleton<IDataDestination, RetryingDataDestination>();
builder.Services.AddSingleton<IMessageSource, FakeMessageSource>();

builder.Services.AddHostedService<SensorProcessorBackgroundService>();

builder.Services.Configure<KafkaOptions>(builder.Configuration);
builder.Services.AddConsumer().AddProducer();

var app = builder.Build();

await app.RunAsync(); 