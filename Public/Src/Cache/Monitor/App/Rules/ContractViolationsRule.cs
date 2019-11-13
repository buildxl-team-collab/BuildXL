using System;
using System.Diagnostics.ContractsLight;
using System.Linq;
using System.Threading.Tasks;
using BuildXL.Cache.ContentStore.Interfaces.Logging;
using Kusto.Data.Common;

namespace BuildXL.Cache.Monitor.App.Rules
{
    internal class ContractViolationsRule : KustoRuleBase
    {
        public class Configuration : KustoRuleConfiguration
        {
            public Configuration(KustoRuleConfiguration kustoRuleConfiguration)
                : base(kustoRuleConfiguration)
            {
            }

            public TimeSpan LookbackPeriod { get; set; } = TimeSpan.FromMinutes(30);
        }

        private readonly Configuration _configuration;

        public override string Identifier => $"{nameof(ContractViolationsRule)}:{_configuration.Environment}/{_configuration.Stamp}";

        public ContractViolationsRule(Configuration configuration)
            : base(configuration)
        {
            Contract.RequiresNotNull(configuration);
            _configuration = configuration;
        }

#pragma warning disable CS0649
        private class Result
        {
            public string ExceptionName;
            public string ExceptionMessage;
            public string Operation;
            public long Machines;
            public long Count;
        }
#pragma warning restore CS0649

        public override async Task Run(RuleContext context)
        {
            var now = _configuration.Clock.UtcNow;
            var query =
                $@"
                let end = now();
                let start = end - {CslTimeSpanLiteral.AsCslString(_configuration.LookbackPeriod)};
                CloudBuildLogEvent
                | where PreciseTimeStamp between (start .. end)
                | where Stamp == ""{_configuration.Stamp}""
                | where Service == ""{Constants.ServiceName}"" or Service == ""{Constants.MasterServiceName}""
                | where Message has ""Critical error occurred:""
                | project PreciseTimeStamp, Machine, Message
                | parse Message with * ""occurred: "" ErrorMessage:string "". Diagnostics: "" Exception:string
                | parse ErrorMessage with Operation:string "" stop "" *
                | parse Exception with ExceptionName:string "": "" ExceptionMessage ""\n"" * // Fetch first line of the exception
                | extend Operation=extract(""^((\\w+).(\\w+)).*"", 1, Operation) // Prune unnecessary parameters
                | project-away Message, ErrorMessage
                | where ExceptionName != ""System.UnauthorizedAccessException""
                | summarize Machines=dcount(Machine, 2), Count=count() by ExceptionName, ExceptionMessage, Operation
                | where not(isnull(Machines))";
            var results = (await QuerySingleResultSetAsync<Result>(query)).ToList();

            if (results.Count == 0)
            {
                // NOTE(jubayard): this is good, there were no critical errors!
                return;
            }

            foreach (var result in results)
            {
                Emit(context, $"ContractViolations_Operation_{result.Operation}", Severity.Error,
                    $"`{result.Machines}` machine(s) had `{result.Count}` contract violations (`{result.ExceptionName}`) in operation `{result.Operation}`. Example message: {result.ExceptionMessage}",
                    $"`{result.Machines}` machine(s) had `{result.Count}` contract violations (`{result.ExceptionName}`) in operation `{result.Operation}`",
                    eventTimeUtc: now);
            }
        }
    }
}
