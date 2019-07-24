﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using BuildXL.Scheduler.Tracing;
using BuildXL.ToolSupport;
using BuildXL.Utilities;
using Google.Protobuf;
using BuildXL.Engine.Cache;
using BuildXL.Engine.Cache.KeyValueStores;
using BuildXL.Engine.Cache.Serialization;
using BuildXL.Native.IO;
using System.Diagnostics;
using BuildXL.Execution.Analyzer.Model;
using BuildXL.Analyzers.Core.XLGPlusPlus;

namespace BuildXL.Execution.Analyzer
{
    internal partial class Args
    {
        public Analyzer InitializeXLGToDBAnalyzer()
        {
            string outputDirPath = null;
            foreach (var opt in AnalyzerOptions)
            {
                if (opt.Name.Equals("outputDir", StringComparison.OrdinalIgnoreCase) ||
                   opt.Name.Equals("o", StringComparison.OrdinalIgnoreCase))
                {
                    outputDirPath = ParseSingletonPathOption(opt, outputDirPath);
                }
                else
                {
                    throw Error("Unknown option for event stats analysis: {0}", opt.Name);
                }
            }


            if (string.IsNullOrEmpty(outputDirPath))
            {
                throw Error("/outputDir parameter is required");
            }


            return new XLGToDBAnalyzer(GetAnalysisInput())
            {
                OutputDirPath = outputDirPath,
            };
        }

        /// <summary>
        /// Write the help message when the analyzer is invoked with the /help flag
        /// </summary>
        private static void WriteXLGToDBHelp(HelpWriter writer)
        {
            writer.WriteBanner("XLG to DB \"Analyzer\"");
            writer.WriteModeOption(nameof(AnalysisMode.XlgToDb), "Dumps event data from the xlg into a database.");
            writer.WriteOption("outputDir", "Required. The directory to write out the RocksDB database", shortName: "o");
        }
    }


    /// <summary>
    /// Analyzer to dump xlg events and other data into RocksDB
    /// </summary>
    internal sealed class XLGToDBAnalyzer : Analyzer
    {
        public string OutputDirPath;
        private bool m_accessorSucceeded;
        private BXLInvocationEventList m_invocationEventList = new BXLInvocationEventList();
        private KeyValueStoreAccessor Accessor { get; set; }
        private uint WorkerID { get; set; }


        public XLGToDBAnalyzer(AnalysisInput input) : base(input) { }

        /// <inheritdoc/>
        public override void Prepare()
        {
            try
            {
                Directory.Delete(path: OutputDirPath, recursive: true);
            }
            catch (Exception e)
            {
                Console.WriteLine("No such dir or could not delete dir with exception {0}.\nIf dir still exists, this analyzer will append data to existing DB.", e);
            }

            var accessor = KeyValueStoreAccessor.Open(storeDirectory: OutputDirPath);

            if (accessor.Succeeded)
            {
                Accessor = accessor.Result;
                m_accessorSucceeded = true;
            }
            else
            {
                Console.Error.WriteLine("Could not access RocksDB datastore. Exiting analyzer.");
            }
        }

        /// <inheritdoc/>
        public override int Analyze()
        {
            if (!m_accessorSucceeded)
            {
                return 0;
            }

            Analysis.IgnoreResult(
              Accessor.Use(database =>
              {
                  foreach (var invEvent in m_invocationEventList.BXLInvEventList)
                  {
                      var eq = new EventTypeQuery
                      {
                          EventTypeID = (int)ExecutionEventId.DominoInvocation,
                          UUID = invEvent.UUID
                      };

                      database.Put(eq.ToByteArray(), invEvent.ToByteArray());
                  }
              })
            );

            Accessor.Dispose();
            return 0;
        }

        /// <inheritdoc/>
        public override bool CanHandleEvent(ExecutionEventId eventId, uint workerId, long timestamp, int eventPayloadSize)
        {
            if (eventId.Equals(ExecutionEventId.DominoInvocation))
            {
                WorkerID = workerId; // store workerID to pass into protobuf object to identify this event
                return true;
            }
            return false; // return false to keep the event from being parsed
        }

        /// <summary>
        /// Override the DominoInvocationEvent to capture its data and store it in the protobuf 
        /// </summary>
        public override void DominoInvocation(DominoInvocationEventData data)
        {
            var domInvEvent = new BXLInvocationEvent();
            var loggingConfig = data.Configuration.Logging;

            var uuid = Guid.NewGuid().ToString();

            domInvEvent.UUID = uuid;
            domInvEvent.WorkerID = WorkerID;
            domInvEvent.SubstSource = loggingConfig.SubstSource.ToString(PathTable, PathFormat.HostOs);
            domInvEvent.SubstTarget = loggingConfig.SubstTarget.ToString(PathTable, PathFormat.HostOs);
            domInvEvent.IsSubstSourceValid = loggingConfig.SubstSource.IsValid;
            domInvEvent.IsSubstTargetValid = loggingConfig.SubstTarget.IsValid;

            m_invocationEventList.BXLInvEventList.Add(domInvEvent);
        }
    }
}
