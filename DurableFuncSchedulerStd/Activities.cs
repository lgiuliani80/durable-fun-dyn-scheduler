using Azure;
using Azure.Data.Tables;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace DurableFuncSchedulerStd
{
    public class Activities
    {
        private readonly ILogger<Activities> _logger;

        private static readonly HttpClient cli = new();

        private static readonly Random rnd = new Random();

        public class Op1Request
        {
            public string Id { get; set; }
            public string Parameter { get; set; }
        }

        public class PendingOperationEntry : ITableEntity
        {
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
            public DateTimeOffset? Timestamp { get; set; }
            [IgnoreDataMember]
            public global::Azure.ETag ETag { get; set; }

            public DateTimeOffset DateCreated { get; set; }
            public DateTimeOffset? DateCompleted { get; set; }
            public string Parameter { get; set; }
            public string Result { get; set; }
        }

        public Activities(ILogger<Activities> log)
        {
            _logger = log;
        }

        [FunctionName("Op1")]
        [return: Queue("op1-requests")]
        public async Task<Op1Request> Op1EnqueueRequest([ActivityTrigger] IDurableActivityContext context, [Table("pendingops")] TableClient tc)
        {
            var myip = await cli.GetStringAsync("https://api.ipify.org");

            var parameter = myip + " - " + context.GetInput<string>();
            var pendingOp = new PendingOperationEntry
            {
                DateCreated = DateTimeOffset.Now,
                Parameter = parameter,
                PartitionKey = "op1",
                RowKey = Guid.NewGuid().ToString()
            };

            await tc.CreateIfNotExistsAsync();
            await tc.AddEntityAsync(pendingOp);

            var retval = new Op1Request { 
                Id = pendingOp.RowKey,
                Parameter = parameter 
            };

            _logger.LogInformation("**** [[{instanceId}]] Enqueueing request OP1 with Parameter='{parameter}', Id={id}",
                context.InstanceId, retval.Parameter, retval.Id);

            return retval;
        }

        [FunctionName("Op1_ProcessRequest")]
        public void Op1ProcessRequest([QueueTrigger("op1-requests")] Op1Request req, [Table("pendingops")] TableClient tc)
        {
            _logger.LogInformation("----------> Op1 {id} with Parameter={parameter} Dequeued - START PROCESSING ....", req.Id, req.Parameter);

            Thread.Sleep(rnd.Next(40) * 1000);

            _logger.LogInformation("<---------- Op1 {id} with Parameter={parameter} COMPLETED PROCESSING", req.Id, req.Parameter);

            try
            {
                var entity = tc.GetEntity<PendingOperationEntry>("op1", req.Id).Value;

                entity.DateCompleted = DateTimeOffset.Now;
                entity.Result = "OK";

                tc.UpdateEntity(entity, entity.ETag);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Unable to find operation {id} in pendingops table", req.Id);
            }
        }

        class AAA { }

        [
            FunctionName("Op1_HttpEnqueueRequest"),
            OpenApiOperation(operationId: "Op1_HttpEnqueueRequest", tags: new[] {"op1"}, Summary = "Enqueues a new 'Op1' operation"), 
            OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query),
            OpenApiParameter("parameter", In = ParameterLocation.Query, Required = false, Type = typeof(string)),
            
            OpenApiResponseWithBody(
                HttpStatusCode.OK,
                contentType: "application/json",
                bodyType: typeof(AAA),
                Description = "OK"
            )
        ]
        public PendingOperationEntry Op1HttpEnqueueRequest(
            [HttpTrigger("post", Route = "op1")] HttpRequest req,
            [Table("pendingops")] TableClient tc,
            [Queue("op1-requests")] out Op1Request dataToEnqueue)
        {
            var op1parameter = req.Query["parameter"].FirstOrDefault() ?? DateTime.Now.ToString();

            var pendingOp = new PendingOperationEntry
            {
                DateCreated = DateTimeOffset.Now,
                Parameter = op1parameter,
                PartitionKey = "op1",
                RowKey = Guid.NewGuid().ToString()
            };

            tc.CreateIfNotExists();
            tc.AddEntity(pendingOp);

            dataToEnqueue = new Op1Request
            {
                Id = pendingOp.RowKey,
                Parameter = op1parameter
            };

            return pendingOp;
        }

        [
            FunctionName("Op1GetOperations"),
            OpenApiOperation(operationId: "Op1_Op1GetOperations", tags: new[] { "op1" }, Summary = "Get ALL operations, both pending and completed"),
            OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query),
            /*
            OpenApiResponseWithBody(
                HttpStatusCode.OK,
                contentType: "application/json",
                bodyType: typeof(PendingOperationEntry[]),
                Description = "OK"
            )*/
        ]
        public object /* IAsyncEnumerable<PendingOperationEntry> */ Op1GetOperations(
            [HttpTrigger("get", Route = "op1")] HttpRequest req,
            [Table("pendingops")] TableClient tc)
        {
            return tc.QueryAsync<PendingOperationEntry>(x => true);
        }

        [
            FunctionName("Op1GetPendingOperations"),
            OpenApiOperation(operationId: "Op1_Op1GetPendingOperations", tags: new[] { "op1" }, Summary = "Get all currently PENDING operations"),
            OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query),
            /*
            OpenApiResponseWithBody(
                HttpStatusCode.OK,
                contentType: "application/json",
                bodyType: typeof(PendingOperationEntry[]),
                Description = "OK"
            )*/
        ]
        public object /* IAsyncEnumerable<PendingOperationEntry> */ Op1GetPendingOperations(
            [HttpTrigger("get", Route = "op1/pending")] HttpRequest req,
            [Table("pendingops")] TableClient tc)
        {
            return tc.QueryAsync<PendingOperationEntry>(x => x.DateCompleted == null);
        }

        [FunctionName("Op1GetPendingOperation")]
        public PendingOperationEntry Op1GetPendingOperation(
            [HttpTrigger("get", Route = "op1/{id:guid}")] HttpRequest req,
            [Table("pendingops", partitionKey: "op1", rowKey: "{id}")] PendingOperationEntry entry)
        {
            return entry;
        }

        #region TIMER: DeleteCompletedOperations
        [FunctionName("DeleteCompletedOperations")]
        public async Task DeleteCompletedOperations(
            [TimerTrigger("0 */5 * * * *", RunOnStartup = true)] TimerInfo deleteCompletedOperationsTimer,
            [Table("pendingops")] TableClient tc, ILogger logger)
        {
            await foreach (var op in tc.QueryAsync<PendingOperationEntry>(x => x.DateCompleted != null))
            {
                await tc.DeleteEntityAsync(op.PartitionKey, op.RowKey);
                logger.LogInformation("Pending operation {id} of type {type} removed", op.RowKey, op.PartitionKey);
            }
        }
        #endregion

    }
}
