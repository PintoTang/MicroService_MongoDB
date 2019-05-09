using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MongoDB.Lib
{
    public static class MongoDBServiceExtensions
    {
        /// <summary>
        /// 配置中心
        /// </summary>
        /// <param name="services"></param>
        /// <param name="configAction"></param>
        /// <returns></returns>
        public static IServiceCollection AddMongoService(this IServiceCollection services, Action<MongoDBHostOptions> configAction)
        {
            if (configAction == null) throw new ArgumentNullException(nameof(configAction));
            services.Configure(configAction);
            return AddMongoService(services);
        }
        /// <summary>
        /// 配置中心
        /// </summary>
        /// <param name="services"></param>
        /// <param name="configAction"></param>
        /// <returns></returns>
        public static IServiceCollection AddMongoService(this IServiceCollection services, IConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            services.Configure<MongoDBHostOptions>(configuration.GetSection("MongodbHost"));
            return AddMongoService(services);
        }
        /// <summary>
        /// 配置中心
        /// </summary>
        /// <param name="services"></param>
        /// <param name="configSetting"></param>
        /// <returns></returns>
        private static IServiceCollection AddMongoService(this IServiceCollection services)
        {
            var service = services.First(x => x.ServiceType == typeof(IConfiguration));
            var configuration = (IConfiguration)service.ImplementationInstance;
            services.AddSingleton<MongoDBClient>();
            services.AddScoped(typeof(IMongoHelper<>), typeof(MongoHelper<>));
            return services;
        }
    }
}
