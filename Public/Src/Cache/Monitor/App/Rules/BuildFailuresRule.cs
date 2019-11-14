using System;
using System.Collections.Generic;
using System.Diagnostics.ContractsLight;
using System.Linq;
using System.Threading.Tasks;
using BuildXL.Cache.ContentStore.Interfaces.Logging;
using Kusto.Data.Common;
using static BuildXL.Cache.Monitor.App.Utilities;

namespace BuildXL.Cache.Monitor.App.Rules
{
    internal class BuildFailuresRule : KustoRuleBase
    {
        public class Configuration : KustoRuleConfiguration
        {
            public Configuration(KustoRuleConfiguration kustoRuleConfiguration)
                : base(kustoRuleConfiguration)
            {
            }

            public TimeSpan LookbackPeriod { get; set; } = TimeSpan.FromHours(1);

            public List<string> CacheErrorBuckets { get; set; } = new List<string>() {
                "PipMaterializeDependenciesFromCacheFailure",
                "PipFailedToMaterializeItsOutputs"
            };

            public Thresholds<double> FailureRateThresholds = new Thresholds<double>()
            {
                Info = 0.1,
                Warning = 0.2,
                Error = 0.4,
                Fatal = 0.5,
            };
        }

        private readonly Configuration _configuration;

        public override string Identifier => $"{nameof(BuildFailuresRule)}:{_configuration.Environment}/{_configuration.Stamp}";

        public BuildFailuresRule(Configuration configuration)
            : base(configuration)
        {
            Contract.RequiresNotNull(configuration);
            _configuration = configuration;
        }

#pragma warning disable CS0649
        private class Result
        {
            public string Stamp;
            public string Ring;
            public string Queue;
            public string BuildId;
            public DateTime BuildStartTime;
            public DateTime BuildEndTime;
            public TimeSpan BuildTotalTime;
            public string DominoSessionId;
            public DateTime MasterInvocationCompletionTime;
            public int ExitCode;
            public string ExitKind;
            public string ErrorBucket;
            public string BucketMessage;
            public bool CacheImplicatedFailure;
        }
#pragma warning restore CS0649

        public override async Task Run(RuleContext context)
        {
            var now = _configuration.Clock.UtcNow;
            var query =
                $@"
                let end = now();
                let start = end - {CslTimeSpanLiteral.AsCslString(_configuration.LookbackPeriod)};
                CacheBuildXLInvocationsWithErrors(""{_configuration.Stamp}"", start, end)
                | extend CacheImplicatedFailure=(ErrorBucket in ({string.Join(",", _configuration.CacheErrorBuckets.Select(b => @$"""{b}"""))}))
                | sort by BuildEndTime desc";
            var results = (await QuerySingleResultSetAsync<Result>(query)).ToList();

            if (results.Count == 0)
            {
                Emit(context, "NoBuilds", Severity.Info,
                    $"Stamp hasn't completed any builds in at least `{_configuration.LookbackPeriod}`",
                    eventTimeUtc: now);
                return;
            }

            var cacheFailures = results.Count(r => r.CacheImplicatedFailure);
            var failureRate = (double)cacheFailures / (double)results.Count;
            _configuration.FailureRateThresholds.Check(failureRate, (severity, threshold) =>
            {
                Emit(context, "FailureRate", severity,
                    $"Build failure rate `{failureRate}` over last `{_configuration.LookbackPeriod}` greater than `{threshold}`",
                    eventTimeUtc: now);
            });
        }
    }
}
