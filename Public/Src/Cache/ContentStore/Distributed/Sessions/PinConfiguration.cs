// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics.ContractsLight;

namespace BuildXL.Cache.ContentStore.Distributed.Sessions
{
    /// <summary>
    /// Contains settings for better pinning logic.
    /// </summary>
    /// <remarks><para>Default settings are highly approximate.</para></remarks>
    public class PinConfiguration
    {
        /// <summary>
        /// Gets or sets the maximum acceptable risk for a successful pin.
        /// </summary>
        public double PinRisk { get; set; } = 1.0E-5;

        /// <summary>
        /// Gets or sets the presumed risk of a machine being inaccessible.
        /// </summary>
        public double MachineRisk { get; set; } = 0.02;

        /// <summary>
        /// Gets or sets the presumed risk of a file being gone from a machine even though the content location record says it should be there.
        /// </summary>
        public double FileRisk { get; set; } = 0.05;

        /// <summary>
        /// Gets or sets the maximum number of simultaneous external file IO operations.
        /// </summary>
        public int MaxIOOperations { get; set; } = 512;

        /// <summary>
        /// Gets or sets the starting retention time for content hash entries in the pin cache.
        /// </summary>
        public int PinCacheReplicaCreditRetentionMinutes { get; set; } = 30;

        /// <summary>
        /// Gets or sets the decay applied for replicas to <see cref="PinCacheReplicaCreditRetentionMinutes"/>. Must be between 0 and 0.9.
        /// For each replica 1...n, with decay d, the additional retention is depreciated by d^n (i.e. only  <see cref="PinCacheReplicaCreditRetentionMinutes"/> * d^n is added to the total retention
        /// based on the replica).
        /// </summary>
        public double PinCacheReplicaCreditRetentionDecay { get; set; } = 0.75;

        /// <summary>
        /// Gets or sets a value indicating whether pin caching should be used
        /// </summary>
        public bool UsePinCache { get; set; }

        public int? MinUnverifiedCount { private get; set; }

        public int? MinVerifiedCount { private get; set; }

        // Compute the minimum number of records for us to proceed with a pin with and without record verification.
        // In this implementation, there are two risks: the machineRisk that a machine cannot be contacted when the file is needed (e.g. network error or service reboot), and
        // the fileRisk that the file is not actually present on the machine despite the record (e.g. the file has been deleted or the machine re-imaged). The second risk
        // can be mitigated by verifying that the files actually exist, but the first cannot. The verifiedRisk of not getting the file from a verified location is thus equal to
        // the machineRisk, while the unverfiedRisk of not getting a file from an unverified location is larger. Given n machines each with risk q, the risk Q of not getting
        // the file from any of them is Q = q^n. Solving for n to get the number of machines required to achieve a given overall risk tolerance gives n = ln Q / ln q.
        // In this way we can compute the minimum number of verified and unverified records to return a successful pin.
        // Future refinements of this method could use machine reputation and file lifetime knowledge to improve this model.
        public void ComputePinThresholds(out int minUnverifiedCount, out int minVerifiedCount)
        {
            if (MinVerifiedCount == null || MinUnverifiedCount == null)
            {
                Contract.Assert((PinRisk > 0.0) && (PinRisk < 1.0));

                double verifiedRisk = MachineRisk;
                double unverifiedRisk = MachineRisk + (FileRisk * (1.0 - MachineRisk));

                Contract.Assert((verifiedRisk > 0.0) && (verifiedRisk < 1.0));
                Contract.Assert((unverifiedRisk > 0.0) && (unverifiedRisk < 1.0));
                Contract.Assert(unverifiedRisk >= verifiedRisk);

                double lnRisk = Math.Log(PinRisk);
                double lnVerifiedRisk = Math.Log(verifiedRisk);
                double lnUnverifiedRisk = Math.Log(unverifiedRisk);

                MinVerifiedCount = (int)Math.Ceiling(lnRisk / lnVerifiedRisk);
                MinUnverifiedCount = (int)Math.Ceiling(lnRisk / lnUnverifiedRisk);
            }

            minUnverifiedCount = MinUnverifiedCount.Value;
            minVerifiedCount = MinVerifiedCount.Value;
        }
    }
}
