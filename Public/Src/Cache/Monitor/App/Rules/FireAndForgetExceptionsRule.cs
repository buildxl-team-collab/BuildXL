﻿using System;
using System.Diagnostics.ContractsLight;
using System.Linq;
using System.Threading.Tasks;
using Kusto.Data.Common;

namespace BuildXL.Cache.Monitor.App.Rules
{
    internal class FireAndForgetExceptionsRule : KustoRuleBase
    {
        public class Configuration : KustoRuleConfiguration
        {
            public Configuration(KustoRuleConfiguration kustoRuleConfiguration)
                : base(kustoRuleConfiguration)
            {
            }

            public TimeSpan LookbackPeriod { get; set; } = TimeSpan.FromMinutes(20);

            public int MachinesWarningThreshold { get; set; } = 10;

            public int MachinesErrorThreshold { get; set; } = 20;

            public int MinimumErrorsThreshold { get; set; } = 20;
        }

        private readonly Configuration _configuration;

        public override string Identifier => $"{nameof(FireAndForgetExceptionsRule)}:{_configuration.Environment}/{_configuration.Stamp}";

        public FireAndForgetExceptionsRule(Configuration configuration)
            : base(configuration)
        {
            Contract.RequiresNotNull(configuration);
            _configuration = configuration;
        }

#pragma warning disable CS0649
        private class Result
        {
            public string Operation;
            public long Machines;
            public long Count;
        }
#pragma warning restore CS0649

        public override async Task Run(RuleContext context)
        {
            // NOTE(jubayard): When a summarize is run over an empty result set, Kusto produces a single (null) row,
            // which is why we need to filter it out.
            var now = _configuration.Clock.UtcNow;
            var query =
                $@"
                let end = now() - {CslTimeSpanLiteral.AsCslString(Constants.KustoIngestionDelay)};
                let start = end - {CslTimeSpanLiteral.AsCslString(_configuration.LookbackPeriod)};
                CloudBuildLogEvent
                | where PreciseTimeStamp between (start .. end)
                | where Stamp == ""{_configuration.Stamp}""
                | where Service == ""{Constants.ServiceName}"" or Service == ""{Constants.MasterServiceName}""
                | where Message has ""Unhandled exception in fire and forget task""
                | where Message !has ""RedisConnectionException"" // This is a transient error (i.e. server closed the socket)
                | where Message !has ""TaskCanceledException"" // This is irrelevant
                | parse Message with * ""operation '"" Operation:string ""'"" * ""FullException="" Exception:string
                | project PreciseTimeStamp, Machine, Operation, Exception
                | summarize Machines=dcount(Machine), Count=count() by Operation
                | where not(isnull(Machines))";
            var results = (await QuerySingleResultSetAsync<Result>(query)).ToList();

            if (results.Count == 0)
            {
                // NOTE(jubayard): this is good, there were no unhandled exceptions!
                return;
            }

            foreach (var result in results)
            {
                Utilities.SeverityFromThreshold(result.Machines, _configuration.MachinesWarningThreshold, _configuration.MachinesErrorThreshold, (severity, threshold) =>
                {
                    if (result.Count < _configuration.MinimumErrorsThreshold)
                    {
                        return;
                    }

                    Emit(context, $"FireAndForgetExceptions_Operation_{result.Operation}", severity,
                        $"`{result.Machines}` machines had `{result.Count}` errors in fire and forget tasks for operation `{result.Operation}`",
                        eventTimeUtc: now);
                });
            }
        }
    }
}
