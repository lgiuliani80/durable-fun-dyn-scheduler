using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Abstractions;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Configurations;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Visitors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Extensions;
using Microsoft.OpenApi.Interfaces;
using Microsoft.OpenApi.Models;
using Microsoft.OpenApi.Writers;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

[assembly: FunctionsStartup(typeof(DurableFuncSchedulerStd.Startup))]
namespace DurableFuncSchedulerStd
{
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddSingleton<IOpenApiConfigurationOptions>(_ =>
            {
                var options = new OpenApiConfigurationOptions
                {
                    Info = new OpenApiInfo
                    {
                        Title = "Dynamic Scheduler APIs",
                        Description = "An example of dynamic scheduler implemented via a Durable Function."
                    }
                };
                return options;
            });
        }
    }
}
