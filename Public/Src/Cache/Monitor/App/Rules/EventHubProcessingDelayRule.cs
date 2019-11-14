using System;
using System.Diagnostics.ContractsLight;
using System.Linq;
using System.Threading.Tasks;
using BuildXL.Cache.ContentStore.Interfaces.Logging;
using Kusto.Data.Common;
using static BuildXL.Cache.Monitor.App.Utilities;

namespace BuildXL.Cache.Monitor.App.Rules
{
    internal class EventHubProcessingDelayRule : KustoRuleBase
    {
        public class Configuration : KustoRuleConfiguration
        {
            public Configuration(KustoRuleConfiguration kustoRuleConfiguration)
                : base(kustoRuleConfiguration)
            {
            }

            public TimeSpan LookbackPeriod { get; set; } = TimeSpan.FromHours(2);

            public TimeSpan BinWidth { get; set; } = TimeSpan.FromMinutes(10);

            public Thresholds<TimeSpan> Thresholds = new Thresholds<TimeSpan>()
            {
                Warning = TimeSpan.FromMinutes(30),
                Error = TimeSpan.FromHours(1),
            };
        }

        private readonly Configuration _configuration;

        public override string Identifier => $"{nameof(EventHubProcessingDelayRule)}:{_configuration.Environment}/{_configuration.Stamp}";

        public EventHubProcessingDelayRule(Configuration configuration)
            : base(configuration)
        {
            Contract.RequiresNotNull(configuration);
            _configuration = configuration;
        }

#pragma warning disable CS0649
        private class Result
        {
            public string Machine;
            public DateTime PreciseTimeStamp;
            public TimeSpan MaxDelay;
            public TimeSpan AvgProcessingDuration;
            public TimeSpan MaxProcessingDuration;
            public long MessageCount;
            public long SumMessageCount;
            public long SumMessageSizeMb;
        }
#pragma warning restore CS0649

        public override async Task Run(RuleContext context)
        {
            var now = _configuration.Clock.UtcNow;
            var query =
                $@"
                let end = now();
                let start = end - {CslTimeSpanLiteral.AsCslString(_configuration.LookbackPeriod)};
                let MasterEvents = CloudBuildLogEvent
                | where PreciseTimeStamp between (start .. end)
                | where Stamp == ""{_configuration.Stamp}""
                | where Service == ""{Constants.MasterServiceName}"";
                let MaximumDelayFromReceivedEvents = MasterEvents
                | where Message has ""ReceivedEvent""
                | parse Message with * ""ProcessingDelay="" Lag:timespan "","" *
                | project PreciseTimeStamp, Stamp, Machine, ServiceVersion, Message, Duration=Lag
                | summarize MaxDelay=max(Duration) by Stamp, PreciseTimeStamp=bin(PreciseTimeStamp, {CslTimeSpanLiteral.AsCslString(_configuration.BinWidth)});
                let MaximumDelayFromEH = MasterEvents
                | where (Message has ""EventHubContentLocationEventStore"" and Message has ""processed"") 
                | parse Message with * ""processed "" MessageCount:long ""message(s) by "" Duration:long ""ms"" * ""TotalMessagesSize="" TotalSize:long *
                | project PreciseTimeStamp, Stamp, MessageCount, Duration, TotalSize, Message, Machine
                | summarize MessageCount=count(), ProcessingDurationMs=percentile(Duration, 50), MaxProcessingDurationMs=max(Duration), SumMessageCount=sum(MessageCount), SumMessageSize=sum(TotalSize) by Stamp, Machine, PreciseTimeStamp=bin(PreciseTimeStamp, {CslTimeSpanLiteral.AsCslString(_configuration.BinWidth)});
                MaximumDelayFromReceivedEvents
                | join MaximumDelayFromEH on Stamp, PreciseTimeStamp
                | project Machine, PreciseTimeStamp, MaxDelay, AvgProcessingDuration=totimespan(ProcessingDurationMs*10000), MaxProcessingDuration=totimespan(MaxProcessingDurationMs*10000), MessageCount, SumMessageCount, SumMessageSizeMb=SumMessageSize / (1024*1024)
                | order by PreciseTimeStamp desc";
            var results = (await QuerySingleResultSetAsync<Result>(query)).ToList();

            if (results.Count == 0)
            {
                Emit(context, "NoLogs", Severity.Fatal,
                    $"No events processed for at least {_configuration.LookbackPeriod}",
                    eventTimeUtc: now);
                return;
            }

            var delay = results[0].MaxDelay;
            _configuration.Thresholds.Check(delay, (severity, threshold) =>
            {
                Emit(context, "DelayThreshold", severity,
                    $"EventHub processing delay `{delay}` above threshold `{threshold}`. Master is {results[0].Machine}",
                    eventTimeUtc: results[0].PreciseTimeStamp);
            });
        }
    }
}
