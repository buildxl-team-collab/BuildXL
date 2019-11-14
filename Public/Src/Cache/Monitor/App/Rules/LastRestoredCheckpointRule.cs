﻿using System;
using System.Collections.Generic;
using System.Diagnostics.ContractsLight;
using System.Linq;
using System.Threading.Tasks;
using BuildXL.Cache.ContentStore.Interfaces.Logging;
using Kusto.Data.Common;
using static BuildXL.Cache.Monitor.App.Utilities;

namespace BuildXL.Cache.Monitor.App.Rules
{
    internal class LastRestoredCheckpointRule : KustoRuleBase
    {
        public class Configuration : KustoRuleConfiguration
        {
            public Configuration(KustoRuleConfiguration kustoRuleConfiguration)
                : base(kustoRuleConfiguration)
            {
            }

            public TimeSpan LookbackPeriod { get; set; } = TimeSpan.FromHours(2);

            public TimeSpan ActivityPeriod { get; set; } = TimeSpan.FromHours(1);

            public Thresholds<int> MissingRestoreMachinesThresholds = new Thresholds<int>() {
                Info = 1,
                Warning = 5,
                Error = 20,
                Fatal = 50,
            };

            public Thresholds<int> OldRestoreMachinesThresholds = new Thresholds<int>()
            {
                Info = 1,
                Warning = 5,
                Error = 20,
                Fatal = 50,
            };

            public TimeSpan CheckpointAgeErrorThreshold { get; set; } = TimeSpan.FromMinutes(45);
        }

        private readonly Configuration _configuration;

        public override string Identifier => $"{nameof(LastRestoredCheckpointRule)}:{_configuration.Environment}/{_configuration.Stamp}";

        public LastRestoredCheckpointRule(Configuration configuration)
            : base(configuration)
        {
            Contract.RequiresNotNull(configuration);
            _configuration = configuration;
        }

#pragma warning disable CS0649
        private class Result
        {
            public string Machine;
            public DateTime LastActivityTime;
            public DateTime? LastRestoreTime;
            public TimeSpan? Age;
        }
#pragma warning restore CS0649

        public override async Task Run(RuleContext context)
        {
            var now = _configuration.Clock.UtcNow;
            var query =
                $@"
                let end = now();
                let start = end - {CslTimeSpanLiteral.AsCslString(_configuration.LookbackPeriod)};
                let activity = end - {CslTimeSpanLiteral.AsCslString(_configuration.ActivityPeriod)};
                let Events = CloudBuildLogEvent
                | where PreciseTimeStamp between (start .. end)
                | where Stamp == ""{_configuration.Stamp}""
                | where Service == ""{Constants.ServiceName}"" or Service == ""{Constants.MasterServiceName}"";
                let Machines = Events
                | where PreciseTimeStamp >= activity
                | summarize LastActivityTime=max(PreciseTimeStamp) by Machine;
                let Restores = Events
                | where Message has ""RestoreCheckpointAsync stop""
                | summarize LastRestoreTime=max(PreciseTimeStamp) by Machine;
                Machines
                | join hint.strategy=broadcast kind=leftouter Restores on Machine
                | project-away Machine1
                | extend Age=LastActivityTime - LastRestoreTime
                | where not(isnull(Machine))";
            var results = (await QuerySingleResultSetAsync<Result>(query)).ToList();

            if (results.Count == 0)
            {
                Emit(context, "NoLogs", Severity.Fatal,
                    $"No machines logged anything in the last `{_configuration.ActivityPeriod}`",
                    eventTimeUtc: now);
                return;
            }

            var missing = new List<Result>();
            var failures = new List<Result>();
            foreach (var result in results)
            {
                if (!result.LastRestoreTime.HasValue)
                {
                    missing.Add(result);
                    continue;
                }

                if (result.Age.Value >= _configuration.CheckpointAgeErrorThreshold)
                {
                    failures.Add(result);
                }
            }

            _configuration.MissingRestoreMachinesThresholds.Check(missing.Count, (severity, threshold) =>
            {
                var formattedMissing = missing.Select(m => $"`{m.Machine}`");
                var machinesCsv = string.Join(", ", formattedMissing);
                var shortMachinesCsv = string.Join(", ", formattedMissing.Take(5));
                Emit(context, "NoRestoresThreshold", severity,
                    $"Found `{missing.Count}` machine(s) active in the last `{_configuration.ActivityPeriod}`, but without checkpoints restored in at least `{_configuration.LookbackPeriod}`: {machinesCsv}",
                    $"`{missing.Count}` machine(s) haven't restored checkpoints in at least `{_configuration.LookbackPeriod}`. Examples: {shortMachinesCsv}",
                    eventTimeUtc: now);
            });

            _configuration.OldRestoreMachinesThresholds.Check(failures.Count, (severity, threshold) =>
            {
                var formattedFailures = failures.Select(f => $"`{f.Machine}` ({f.Age.Value})");
                var machinesCsv = string.Join(", ", formattedFailures);
                var shortMachinesCsv = string.Join(", ", formattedFailures.Take(5));
                Emit(context, "OldRestores", severity,
                    $"Found `{failures.Count}` machine(s) active in the last `{_configuration.ActivityPeriod}`, but with old checkpoints (at least `{_configuration.CheckpointAgeErrorThreshold}`): {machinesCsv}",
                    $"`{failures.Count}` machine(s) have checkpoints older than `{_configuration.CheckpointAgeErrorThreshold}`. Examples: {shortMachinesCsv}",
                    eventTimeUtc: now);
            });
        }
    }
}
