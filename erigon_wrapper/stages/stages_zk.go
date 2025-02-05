package stages

var (
	// ZK stages
	L1Syncer                    SyncStage = "L1Syncer"
	L1SequencerSyncer           SyncStage = "L1SequencerSyncer"
	L1VerificationsBatchNo      SyncStage = "L1VerificationsBatchNo"
	Batches                     SyncStage = "Batches"
	HighestHashableL2BlockNo    SyncStage = "HighestHashableL2BlockNo"
	HighestSeenBatchNumber      SyncStage = "HighestSeenBatchNumber"
	VerificationsStateRootCheck SyncStage = "VerificationStateRootCheck"
	ForkId                      SyncStage = "ForkId"
	L1SequencerSync             SyncStage = "L1SequencerSync"
	L1InfoTree                  SyncStage = "L1InfoTree"
	// HighestUsedL1InfoIndex      SyncStage = "HighestUsedL1InfoTree"
	SequenceExecutorVerify SyncStage = "SequenceExecutorVerify"
	L1BlockSync            SyncStage = "L1BlockSync"
	Witness                SyncStage = "Witness"
)
