using System;
using System.Collections.Generic;
using System.Diagnostics.ContractsLight;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using BuildXL.Cache.ContentStore.Interfaces.Logging;
using Kusto.Data.Common;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace BuildXL.Cache.Monitor.App.Rules
{
    internal class OperationPerformanceCheckRule : KustoRuleBase
    {
        public class Check
        {
            public TimeSpan LookbackPeriod { get; set; } = TimeSpan.FromDays(1);

            public string Match { get; set; }

            private string _name = null;

            public string Name
            {
                get => _name ?? Match;
                set => _name = value;
            }

            public long MinimumRowsPerMachine = 64;
                
            public long Limit { get; set; } = 500000;

            public double AnomalyThreshold { get; set; } = 0.3;
        }

        public class Configuration : KustoRuleConfiguration
        {
            public Configuration(KustoRuleConfiguration kustoRuleConfiguration)
                : base(kustoRuleConfiguration)
            {
            }

            public Check Check { get; set; }
        }

        private readonly Configuration _configuration;

        public override string Identifier => $"{nameof(OperationPerformanceCheckRule)};{_configuration.Check.Name}:{_configuration.Environment}/{_configuration.Stamp}";

        public OperationPerformanceCheckRule(Configuration configuration)
            : base(configuration)
        {
            Contract.RequiresNotNull(configuration);
            Contract.RequiresNotNull(configuration.Check);
            _configuration = configuration;
        }

#pragma warning disable CS0649
        private class Result
        {
            public DateTime PreciseTimeStamp;
            public string Machine;
            public string OperationId;
            public string Operation;
            public float TimeMs;
        }

        private class Output
        {
            /// <summary>
            /// Three entries, all regarding the data point at the same index as this object:
            ///     1. Whether this is an anomaly or not
            ///     2. Score, used to threshold and choose if something is an anomaly or not
            ///     3. p-value, the closer to 0 the more confidence in the score's value
            /// </summary>
            [VectorType(3)]
            public double[] Prediction { get; set; }
        }
#pragma warning restore CS0649

        public override async Task Run(RuleContext context)
        {
            var now = _configuration.Clock.UtcNow;
            var query =
                $@"
                let end = now();
                let start = end - {CslTimeSpanLiteral.AsCslString(_configuration.Check.LookbackPeriod)};
                CloudBuildLogEvent
                | where PreciseTimeStamp between (start .. end)
                | where Stamp == ""{_configuration.Stamp}""
                | where Service == ""{Constants.ServiceName}"" or Service == ""{Constants.MasterServiceName}""
                | where Message has ""{_configuration.Check.Match} stop ""
                | where Message has ""result=[Success""
                | project PreciseTimeStamp, Machine, Message
                | parse Message with OperationId:string "" "" Operation:string "" stop "" TimeMs:double ""ms. "" *
                | extend Operation=extract(""^((\\w+).(\\w+)).*"", 1, Operation) // Prune unnecessary parameters
                | project-away Message
                | sort by PreciseTimeStamp desc, Machine desc
                | take {_configuration.Check.Limit}";
            var results = (await QuerySingleResultSetAsync<Result>(context, query)).ToList();

            if (results.Count == 0)
            {
                return;
            }

            var reports = results
                .AsParallel()
                .GroupBy(result => result.Machine)
                // No need to sort again, groupby preserves order
                .SelectMany(group => ComputeReport(group.ToList()))
                .AsSequential()
                .OrderByDescending(entry => entry.Result.PreciseTimeStamp);

            foreach (var entry in reports)
            {
                Emit(context, $"Performance_{_configuration.Check.Match}_{entry.Result.Operation}", Severity.Info,
                    $"Operation `{entry.Result.Operation}` took `{entry.Result.TimeMs}ms` to complete on `{entry.Result.Machine}`. Id: {entry.Result.OperationId}. Scores: {entry.Prediction[0]}, {entry.Prediction[1]}, {entry.Prediction[2]}",
                    eventTimeUtc: entry.Result.PreciseTimeStamp);
            }
        }

        private IEnumerable<(Result Result, double[] Prediction)> ComputeReport(List<Result> results)
        {
            if (results.Count < _configuration.Check.MinimumRowsPerMachine)
            {
                return Array.Empty<(Result Result, double[] Prediction)>();
            }

            var predictions = DetectAnomalies(results);

            return results
                .Zip(predictions, (r, p) => (Result: r, p.Prediction))
                .Where(t => t.Prediction[0] > 0);
        }

        private IEnumerable<Output> DetectAnomalies(List<Result> results)
        {
            var mlContext = new MLContext(seed: 0);

            var dataView = mlContext.Data.LoadFromEnumerable(results);

            var estimator = mlContext.Transforms.DetectAnomalyBySrCnn(
                outputColumnName: nameof(Output.Prediction),
                inputColumnName: nameof(Result.TimeMs),
                windowSize: 34,
                backAddWindowSize: 5,
                lookaheadWindowSize: 5,
                averageingWindowSize: 3,
                judgementWindowSize: 21,
                threshold: _configuration.Check.AnomalyThreshold);

            var model = estimator.Fit(dataView);

            return mlContext.Data.CreateEnumerable<Output>(model.Transform(dataView), reuseRowObject: false);
        }
    }
}
