using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Data.Tables;
using Cronos;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json;
using static DurableFuncSchedulerStd.ScheduleListHttpApis;

namespace DurableFuncSchedulerStd
{
    public class DurableScheduler
    {
        public const string ORCHESTRATOR_SINGLETON_NAME = "DURABLE_SCHEDULER_SINGLETON";
        public const string SCHEDULES_UPDATED_EVENT_NAME = "SCHEDULES_UPDATED";

        private readonly ILogger<ScheduleListHttpApis> _logger;

        public class CheckStatusResponse
        {
            public string Id { get; set; }
            public string PurgeHistoryDeleteUri { get; set; }
            public string SendEventPostUri { get; set; }
            public string StatusQueryGetUri { get; set; }
            public string TerminatePostUri { get; set; }
        }

        public DurableScheduler(ILogger<ScheduleListHttpApis> log)
        {
            _logger = log;
        }

        #region ORCHESTRATOR
        [FunctionName("DurableScheduler")]
        public async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context
        )
        {
            DateTime firstDeadline;
            var nextDeadlines = await context.CallActivityAsync<SortedDictionary<DateTime, List<ScheduleObjectInternal>>>("DurableScheduler_GetDeadlines", null);
            
            if (nextDeadlines.Keys.Any())
            {
                firstDeadline = nextDeadlines.Keys.First();

                if (!context.IsReplaying)
                {
                    _logger.LogInformation("**** [[{instanceId}]] Waiting for next deadline @ {deadline} for {crons} => {operations}",
                        context.InstanceId, firstDeadline,
                        string.Join(", ", nextDeadlines[firstDeadline].Select(z => z.Cron)),
                        string.Join(", ", nextDeadlines[firstDeadline].Select(z => z.Operation))
                    );
                }
            } 
            else
            {
                firstDeadline = (DateTime.UtcNow + TimeSpan.FromHours(1));

                if (!context.IsReplaying)
                {
                    _logger.LogInformation("**** [[{instanceId}]] Waking up next hour for schedules reload @ {deadline}",
                        context.InstanceId, firstDeadline);
                }
            }

            var timerCancelSource = new CancellationTokenSource();
            var externalEventCancelSource = new CancellationTokenSource();

            var externalEventTriggered = context.WaitForExternalEvent(SCHEDULES_UPDATED_EVENT_NAME, TimeSpan.FromDays(1), externalEventCancelSource.Token);
            var firstDeadlineOccurred = context.CreateTimer(firstDeadline, timerCancelSource.Token);

            var eventOccurred = await Task.WhenAny(externalEventTriggered, firstDeadlineOccurred);

            if (eventOccurred == firstDeadlineOccurred)
            {
                externalEventCancelSource.Cancel();

                var firstKey = nextDeadlines.Keys.Any() ? nextDeadlines.Keys.First() : DateTime.MaxValue;

                while (firstKey <= DateTime.UtcNow)
                {
                    foreach (var op in nextDeadlines[firstKey])
                    {
                        if (!context.IsReplaying)
                            _logger.LogWarning("**** [[{instanceId}]] ### Operation triggered for {cron}: {operation}", context.InstanceId, op.Cron, op.Operation);
                    
                        if (op.Operation.StartsWith("!"))
                        {
                            var activityToCall = op.Operation[1..];

                            if (!context.IsReplaying)
                                _logger.LogInformation("**** [[{instanceId}]] --- Launching activity '{activity}'", context.InstanceId, activityToCall);

                            await context.CallActivityAsync(activityToCall, $"[DEADLINE={firstKey}]");
                        }
                    }

                    nextDeadlines.Remove(firstKey);
                    firstKey = nextDeadlines.Keys.Any() ? nextDeadlines.Keys.First() : DateTime.MaxValue;
                }
            }
            else
            {
                _logger.LogWarning("**** [[{instanceId}]] !!! External event received: schedules list updated. Reprocessing them.", context.InstanceId);
                timerCancelSource.Cancel();
            }

            context.ContinueAsNew(null);
        }
        #endregion

        #region ACTIVITY: GetDeadlines
        [FunctionName("DurableScheduler_GetDeadlines")]
        public SortedDictionary<DateTime, List<ScheduleObjectInternal>> GetDeadlines(
            [ActivityTrigger] IDurableActivityContext context,
            [Table("Schedules")] TableClient scheduleTbl)
        {
            SortedDictionary<DateTime, List<ScheduleObjectInternal>> nextDeadlines = new();
            int count = 0;

            _logger.LogInformation("$$$$ [[{instanceId}]] Starting reading schedules @ {utcnow}", context.InstanceId, DateTime.UtcNow);

            foreach (var i in scheduleTbl.Query<ScheduleObjectInternal>(x => true))
            {
                var expr = CronExpression.Parse(i.Cron);
                var nextOccurrence = expr.GetNextOccurrence(DateTime.UtcNow + TimeSpan.FromSeconds(1));
                if (nextOccurrence != null)
                {
                    if (!nextDeadlines.ContainsKey(nextOccurrence.Value))
                    {
                        nextDeadlines[nextOccurrence.Value] = new List<ScheduleObjectInternal> { i };
                    }
                    else
                    {
                        nextDeadlines[nextOccurrence.Value].Add(i);
                    }
                }
                count++;
            }

            _logger.LogInformation("$$$$ [[{instanceId}]] Number of total schedules: {scheduleCount}; Next 10 deadlines: {nextDeadlines}",
                context.InstanceId,
                count, 
                string.Join(", ", nextDeadlines.Take(10).SelectMany(x => x.Value.Select(y => $"{x.Key:o}:'{y.Cron}'='{y.Operation}'")))
            );

            return nextDeadlines;
        }
        #endregion

        #region API: POST startOrchestrator
        [
            FunctionName("DurableScheduler_HttpStart"),
            OpenApiOperation(
                operationId: "DurableScheduler_HttpStart", 
                tags: new[] { "Orchestrators" },
                Summary = "Starts a new Dynamic Schedules Orchestrator. WARNING: there must be only ONE of such orchestrators running!"
            ), 
            OpenApiParameter(
                "name", 
                In = ParameterLocation.Path, 
                Required = false, 
                Description = "Name of the orchestrator instance. If omitted 'DURABLE_SCHEDULER_SINGLETON' will be used."
            ),
            OpenApiResponseWithBody(
                HttpStatusCode.OK,
                contentType: "application/json",
                bodyType: typeof(DurableOrchestrationStatus),
                Description = "A new orchestrator was created"
            ),
            OpenApiResponseWithoutBody(
                HttpStatusCode.NoContent,
                Description = "The specified orchestrator was already running"
            )
        ]
        public async Task<HttpResponseMessage> StartOrchestrator(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "startOrchestrator/{name?}")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient client,
            string name = ORCHESTRATOR_SINGLETON_NAME)
        {
            var existingInstance = await client.GetStatusAsync(name);

            switch (existingInstance?.RuntimeStatus ?? OrchestrationRuntimeStatus.Unknown)
            {
                case OrchestrationRuntimeStatus.Completed:
                case OrchestrationRuntimeStatus.Terminated:
                case OrchestrationRuntimeStatus.Failed:
                case OrchestrationRuntimeStatus.Unknown:
                    string instanceId = await client.StartNewAsync("DurableScheduler", name);
                    _logger.LogInformation($"Started orchestration with ID = '{instanceId}'.");
                    return client.CreateCheckStatusResponse(req, instanceId);

                default:
                    return new HttpResponseMessage(HttpStatusCode.NoContent);
            }
        }
        #endregion

        #region API: GET orchestrators
        [
            FunctionName("DurableScheduler_GetOrchestrators"),
            OpenApiOperation(
                operationId: "DurableScheduler_GetOrchestrators", 
                tags: new[] { "Orchestrators" },
                Summary = "Get the running orchestrators"
            ), 
            OpenApiResponseWithBody(
                HttpStatusCode.OK,
                contentType: "application/json",
                bodyType: typeof(IEnumerable<DurableOrchestrationStatus>),
                Description = "The list of orchestrations in execution"
            )
        ]
        public async Task<IActionResult> GetOrchestrators(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "orchestrators")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient client)
        {
            var noFilter = new OrchestrationStatusQueryCondition();
            var result = await client.ListInstancesAsync(noFilter, CancellationToken.None);

            return new OkObjectResult(result.DurableOrchestrationState);
        }
        #endregion

        #region API: DELETE orchestrators/{instanceId}
        [
            FunctionName("DurableScheduler_TerminateOrchestrator"),
            OpenApiOperation(
                operationId: "DurableScheduler_TerminateOrchestrator", 
                tags: new[] { "Orchestrators" },
                Summary = "Terminate an orchestrator by instance id"
            ), 
            OpenApiParameter("id", In = ParameterLocation.Path, Required = true, Description = "Orchestrator Id to terminate"),
            OpenApiResponseWithoutBody(
                HttpStatusCode.NoContent,
                Description = "The operation completed successfully"
            )
        ]
        public async Task<IActionResult> TerminateOrchestrator(
            [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "orchestrators/{id}")] HttpRequestMessage req,
            string id,
            [DurableClient] IDurableOrchestrationClient client)
        {
            try
            {
                await client.TerminateAsync(id, "User requested");
            }
            catch { }
            try
            {
                await client.PurgeInstanceHistoryAsync(id);
            }
            catch { }

            return new NoContentResult();
        }
        #endregion

        #region TIMER: EnsureOrchestrationRunning
        [FunctionName("EnsureOrchestrationRunning")]
        public async Task EnsureOrchestrationRunning(
            [TimerTrigger("0 * * * * *", RunOnStartup = true)] TimerInfo checkOrchestratorRunningTimer,
            [DurableClient] IDurableOrchestrationClient client)
        {
            var existingInstance = await client.GetStatusAsync(ORCHESTRATOR_SINGLETON_NAME);

            switch (existingInstance?.RuntimeStatus ?? OrchestrationRuntimeStatus.Unknown)
            {
                case OrchestrationRuntimeStatus.Completed:
                case OrchestrationRuntimeStatus.Terminated:
                case OrchestrationRuntimeStatus.Failed:
                case OrchestrationRuntimeStatus.Unknown:
                    string instanceId = await client.StartNewAsync("DurableScheduler", ORCHESTRATOR_SINGLETON_NAME);
                    _logger.LogInformation($"EnsureOrchestrationRunning: started singleton orchestrator '{instanceId}'.");
                    break;

                default:
                    _logger.LogInformation("EnsureOrchestrationRunning: orchestrator was already running");
                    break;
            }
        }
        #endregion
    }
}