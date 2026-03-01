using Microsoft.Extensions.DependencyInjection;
using SensorDataConsumer.Background;
using SensorDataConsumer.Services;

namespace SensorDataConsumer.Extensions;

public static class KafkaExtensions
{
    extension(IServiceCollection services)
    {
        public IServiceCollection AddProducer()
        {
            services.AddSingleton<SensorDataProducer>();
            services.AddSingleton<KafkaSensorDataProducer>();

            services.AddHostedService<KafkaConsumer>();

            return services;
        }

        public IServiceCollection AddConsumer()
        {
            services.AddSingleton<KafkaSensorDataConsumer>();

            services.AddHostedService<KafkaProducer>();

            return services;
        }
    }
}