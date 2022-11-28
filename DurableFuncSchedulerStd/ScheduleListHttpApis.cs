using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Azure.Data.Tables;
using System;
using ITableEntity = Azure.Data.Tables.ITableEntity;
using Azure;
using System.Collections.Generic;
using System.Linq;
using NCrontab;
using Cronos;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;

namespace DurableFuncSchedulerStd
{
    public class ScheduleListHttpApis
    {
        private readonly ILogger<ScheduleListHttpApis> _logger;

        public ScheduleListHttpApis(ILogger<ScheduleListHttpApis> log)
        {
            _logger = log;
        }

        public class ScheduleObjectInternal : ITableEntity
        {
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
            public DateTimeOffset? Timestamp { get; set; }
            public ETag ETag { [OpenApiIgnore] get; [OpenApiIgnore] set; }

            public string Cron { get; set; }
            public string Operation { get; set; }
            public DateTimeOffset TimeCreated { get; set; }
        }

        public class ScheduleObject
        {
            public string Cron { get; set; }
            public string Operation { get; set; }
        }

        public class ScheduleObjectId : ScheduleObject
        {
            public string Id { get; set; }
        }

        public class ScheduleCount
        {
            public int Count { get; set; }
        }

        public record ScheduleId(string Id);

        #region APU: POST schedule
        [
            FunctionName("AddSchedule"), 
            OpenApiOperation(
                operationId: "AddSchedule", 
                tags: new[] { "DynamicScheduleCRUD" },
                Summary = "Add a new schedules operation"
            ), 
            OpenApiRequestBody("application/json", typeof(ScheduleObject)),
            OpenApiResponseWithBody(
                HttpStatusCode.Created, 
                contentType: "application/json", 
                bodyType: typeof(ScheduleId), 
                Description = "The id of the created object"
            ),
            OpenApiResponseWithoutBody(
                HttpStatusCode.BadRequest,
                Description = "The specified cron string is not a valid crontab string"
            )
        ]
        public async Task<IActionResult> AddSchedule(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "schedule")] HttpRequest req,
            [Table("Schedules")] TableClient scheduleTbl,
            [DurableClient] IDurableOrchestrationClient client)
        {
            var soRecv = JsonConvert.DeserializeObject<ScheduleObject>(await req.ReadAsStringAsync());
            var so = new ScheduleObjectInternal { Cron = soRecv.Cron, Operation = soRecv.Operation };

            try
            {
                CronExpression.Parse(so.Cron);
            } 
            catch
            {
                return new BadRequestResult();
            }

            so.PartitionKey = "1";
            so.RowKey = Guid.NewGuid().ToString();
            so.TimeCreated = DateTimeOffset.Now;

            await scheduleTbl.CreateIfNotExistsAsync();
            await scheduleTbl.AddEntityAsync(so);

            try
            {
                await client.RaiseEventAsync(DurableScheduler.ORCHESTRATOR_SINGLETON_NAME, DurableScheduler.SCHEDULES_UPDATED_EVENT_NAME);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Unable to deliver {DurableScheduler.SCHEDULES_UPDATED_EVENT_NAME} to orchestrator {DurableScheduler.ORCHESTRATOR_SINGLETON_NAME}");
            }

            return new CreatedResult($"schedule/{so.RowKey}", new ScheduleId(so.RowKey));
        }
        #endregion

        #region API: PUT schedule
        [
            FunctionName("UpdateSchedule"),
            OpenApiOperation(
                operationId: "UpdateSchedule", 
                tags: new[] { "DynamicScheduleCRUD" },
                Summary = "Update an existing scheduled operation"
            ), 
            OpenApiRequestBody("application/json", typeof(ScheduleObject)),
            OpenApiParameter(
                "id", 
                In = ParameterLocation.Path, 
                Required = true, 
                Type = typeof(string), 
                Description = "The id of the entity to retrieve"
            ),
            OpenApiResponseWithBody(
                HttpStatusCode.Created, 
                contentType: "application/json", 
                bodyType: typeof(ScheduleObject), 
                Description = "The updated entity"
            ),
            OpenApiResponseWithoutBody(
                HttpStatusCode.BadRequest,
                Description = "The specified cron string is not a valid crontab string"
            )
        ]
        public async Task<IActionResult> UpdateSchedule(
            [HttpTrigger(AuthorizationLevel.Function, "put", Route = "schedule/{id:guid}")] HttpRequest req,
            string id,
            [Table("Schedules")] TableClient scheduleTbl,
            [Table("Schedules", "1", "{id}")] ScheduleObjectInternal item,
            [DurableClient] IDurableOrchestrationClient client)
        {
            var soRecv = JsonConvert.DeserializeObject<ScheduleObject>(await req.ReadAsStringAsync());
            var so = new ScheduleObjectInternal {
                PartitionKey = "1",
                RowKey = id,
                Cron = soRecv.Cron ?? item?.Cron,
                Operation = soRecv.Operation ?? item?.Operation,
                TimeCreated = item?.TimeCreated ?? DateTimeOffset.Now
            };

            try
            {
                CronExpression.Parse(so.Cron);
            }
            catch
            {
                return new BadRequestResult();
            }

            await scheduleTbl.UpsertEntityAsync(so);

            try
            {
                await client.RaiseEventAsync(DurableScheduler.ORCHESTRATOR_SINGLETON_NAME, DurableScheduler.SCHEDULES_UPDATED_EVENT_NAME);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Unable to deliver {DurableScheduler.SCHEDULES_UPDATED_EVENT_NAME} to orchestrator {DurableScheduler.ORCHESTRATOR_SINGLETON_NAME}");
            }

            return new OkObjectResult(so);
        }
        #endregion

        #region API: GET schedule/count
        [
            FunctionName("GetSchedulesCount"),
            OpenApiOperation(
                operationId: "GetSchedulesCount", 
                tags: new[] { "DynamicScheduleCRUD" },
                Summary = "Get the total number of schedules operations"
            ), 
            OpenApiResponseWithBody(
                HttpStatusCode.OK,
                contentType: "application/json",
                bodyType: typeof(ScheduleCount),
                Description = "The number of schedules saves"
            )
        ]
        public async Task<IActionResult> GetSchedulesCount(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "schedule/count")] HttpRequest req,
            [Table("Schedules")] TableClient scheduleTbl)
        {
            int count = 0;

            await foreach (var i in scheduleTbl.QueryAsync<ScheduleObjectInternal>(x => true))
            {
                count++;
            }

            return new OkObjectResult(new ScheduleCount { Count = count });
        }
        #endregion

        #region API: GET schedule/{id}
        [
            FunctionName("GetSchedule"),
            OpenApiOperation(
                operationId: "GetSchedule", 
                tags: new[] { "DynamicScheduleCRUD" },
                Summary = "Get a specific schedule by id"
            ), 
            OpenApiParameter(
                "id",
                In = ParameterLocation.Path,
                Required = true,
                Type = typeof(string),
                Description = "The id of the entity to retrieve"
            ),
            OpenApiResponseWithBody(
                HttpStatusCode.OK, 
                contentType: "application/json", 
                bodyType: typeof(ScheduleObject), 
                Description = "The schedule object"
            ),
            OpenApiResponseWithoutBody(
                HttpStatusCode.NotFound,
                Description = "The requested item was not found"
            )
        ]
        public async Task<IActionResult> GetSchedule(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "schedule/{id:guid}")] HttpRequest req,
            string id,
            [Table("Schedules", "1", "{id}")] ScheduleObjectInternal item)
        {
            await Task.CompletedTask;

            if (item == null)
                return new NotFoundResult();
            
            return new OkObjectResult(new ScheduleObject { Cron = item.Cron, Operation = item.Operation });
        }
        #endregion

        #region API: GET schedule [all]
        [
            FunctionName("GetSchedules"),
            OpenApiOperation(
                operationId: "GetSchedules", 
                tags: new[] { "DynamicScheduleCRUD" },
                Summary = "Get all scheduled operations"
            ), 
            OpenApiResponseWithBody(
                HttpStatusCode.OK, 
                contentType: "application/json", 
                bodyType: typeof(ScheduleObject[]), 
                Description = "The schedule objects"
            )
        ]
        public async IAsyncEnumerable<ScheduleObject> GetSchedules(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "schedule")] HttpRequest req,
            [Table("Schedules")] TableClient scheduleTbl)
        {
            await foreach (var i in scheduleTbl.QueryAsync<ScheduleObjectInternal>(x => true))
            {
                yield return new ScheduleObjectId { Id = i.RowKey, Cron = i.Cron, Operation = i.Operation };
            }
        }
        #endregion

        #region API: DELETE schedule/{id}
        [
            FunctionName("DeleteSchedule"),
            OpenApiOperation(
                operationId: "DeleteSchedule", 
                tags: new[] { "DynamicScheduleCRUD" },
                Summary = "Delete a specific scheduled operation by id"
            ), 
            OpenApiParameter(
                "id",
                In = ParameterLocation.Path,
                Required = true,
                Type = typeof(string),
                Description = "The id of the entity to retrieve"
            ),
            OpenApiResponseWithoutBody(
                HttpStatusCode.NoContent, 
                Description = "Operation completed successfully"
            ),
            OpenApiResponseWithoutBody(
                HttpStatusCode.NotFound,
                Description = "The requested item was not found"
            )
        ]
        public async Task<IActionResult> DeleteSchedule(
            [HttpTrigger(AuthorizationLevel.Function, "delete", Route = "schedule/{id}")] HttpRequest req,
            string id,
            [Table("Schedules")] TableClient scheduleTbl,
            [DurableClient] IDurableOrchestrationClient client)
        {
            try
            {
                await scheduleTbl.DeleteEntityAsync("1", id);
                try
                {
                    await client.RaiseEventAsync(DurableScheduler.ORCHESTRATOR_SINGLETON_NAME, DurableScheduler.SCHEDULES_UPDATED_EVENT_NAME);
                } 
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, $"Unable to deliver {DurableScheduler.SCHEDULES_UPDATED_EVENT_NAME} to orchestrator {DurableScheduler.ORCHESTRATOR_SINGLETON_NAME}");
                }
                return new NoContentResult();
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }
        #endregion
    }
}

