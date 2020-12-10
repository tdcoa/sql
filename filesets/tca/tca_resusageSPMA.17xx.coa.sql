/* TCA Process for ResUsageSPMA == 1710 */ 
/*  Parameters:
     startdate = {startdate}
     enddate = {enddate}
     resusagespma = {resusagespma}
     dbprefix = {dbprefix} <-- probably blank, but set to "Dev_" in the FileSet during TCA Testing
     dbsversion_label = {dbsversion_label}
 */
 
/*{{save:tca_resusageSVPR_{dbsversion_label}.csv}}*/
/*{{load:{dbprefix}APP_TCA_TMP.stg_tca_resusageSPMA_{dbsversion_label}}}*/
/*{{call:{dbprefix}APP_TCA_TBL.sp_tca_resusageSPMA_{dbsversion_label}()}}*/
Select 
  Site_ID
 ,TheDate
 ,NodeID
 ,TheTime
 ,GmtTime
 ,CabinetID
 ,ModuleID
 ,NodeType
 ,TheTimestamp
 ,CentiSecs
 ,Secs
 ,NominalSecs
 ,PM_COD_CPU
 ,PM_COD_IO
 ,WM_COD_CPU
 ,WM_COD_IO
 ,TIER_FACTOR
 ,Reserved00
 ,ReservedS0
 ,NCPUs
 ,Vproc1
 ,Vproc2
 ,Vproc3
 ,Vproc4
 ,Vproc5
 ,Vproc6
 ,Vproc7
 ,VprocType1
 ,VprocType2
 ,VprocType3
 ,VprocType4
 ,VprocType5
 ,VprocType6
 ,VprocType7
 ,NodeNormFactor
 ,MemSize
 ,ReservedS1
 ,ProcPendMisc
 ,ProcBlksTime
 ,ProcWaitMsgRead
 ,ProcWaitTime
 ,ProcWaitMisc
 ,MemFreeKB
 ,MsgPtPReadKB
 ,MsgPtPWriteKB
 ,MsgBrdReadKB
 ,MsgBrdWriteKB
 ,NetMsgPtPWriteKB
 ,NetMsgBrdWriteKB
 ,NetMsgPtPReadKB
 ,NetMsgBrdReadKB
 ,NetRxKBPtP
 ,NetTxKBPtP
 ,NetRxKBBrd
 ,NetTxKBBrd
 ,NetMrgTxKB
 ,NetMrgRxKB
 ,NetMrgTxRows
 ,NetMrgRxRows
 ,HostReadKB
 ,HostWriteKB
 ,FileAcqKB
 ,FileAcqOtherKB
 ,FileAcqReadKB
 ,FileRelKB
 ,FileRelOtherKB
 ,FileWriteKB
 ,FilePreKB
 ,FilePreReadKB
 ,FileContigWIos
 ,FileContigWBlocks
 ,FileContigWKB
 ,PSQWaitTime
 ,PSServiceTime
 ,TvsReadCnt
 ,TvsWriteCnt
 ,TvsReadRespTot
 ,TvsWriteRespTot
 ,FullPotentialIota
 ,CodPotentialIota
 ,UsedIota
 ,CpuThrottleCount
 ,CpuThrottleTime
 ,VHCacheKB
 ,NLBMsgFlowControlledKB
 ,NetActiveMrgKB
 ,KernMemInuseKB
 ,SegMDLInuseKB
 ,FsgCacheKB
 ,PageScanDirects
 ,PageScanKswapds
 ,PageMajorFaults
 ,PageMinorFaults
 ,SlabCacheKB
 ,DiskCacheInUseKB
 ,DiskCacheTotalKB
 ,PageCacheKB
 ,IoThrottleCount
 ,IoThrottleTime
 ,IoThrottleTimeMax
 ,IoThrottleCntZeroIotas
 ,IoThrottleCntInsuffIotas
 ,IoThrottleCntInsuffIotasHL
 ,IoThrottleCntMaxQD
 ,NosTables
 ,NosPhysReadIOs
 ,NosPhysReadIOKB
 ,NosFiles
 ,NosFilesSkipped
 ,NosRecordsReturned
 ,NosRecordsReturnedKB
 ,NosRecordsSkipped
 ,NosTotalIOWaitTime
 ,NosMaxIOWaitTime
 ,NosCPUTime
 ,Spare00
 ,Spare01
 ,Spare02
 ,Spare03
 ,Spare04
 ,Spare05
 ,Spare06
 ,Spare07
 ,Spare08
 ,Spare09
 ,Spare10
 ,Spare11
 ,Spare12
 ,Spare13
 ,Spare14
 ,Spare15
 ,Spare16
 ,Spare17
 ,Spare18
 ,Spare19
 ,Active
 ,ProcReadyMax
 ,ProcReady
 ,ProcBlocked
 ,ProcPendMemAlloc
 ,ProcPendFsgRead
 ,ProcPendFsgWrite
 ,ProcPendNetThrottle
 ,ProcPendNetRead
 ,ProcPendMonitor
 ,ProcPendMonResume
 ,ProcPendDBLock
 ,ProcPendSegLock
 ,ProcPendFsgLock
 ,ProcPendQnl
 ,ProcBlksMemAlloc
 ,ProcBlksFsgRead
 ,ProcBlksFsgWrite
 ,NetSemInUse
 ,NetChanInUse
 ,NetGroupInUse
 ,ProcBlksNetThrottle
 ,ProcBlksMsgRead
 ,ProcBlksMonitor
 ,ProcBlksMonResume
 ,ProcBlksDBLock
 ,ProcBlksSegLock
 ,ProcBlksMisc
 ,ProcBlksFsgLock
 ,ProcBlksQnl
 ,ProcWaitMemAlloc
 ,ProcWaitPageRead
 ,ProcWaitFsgRead
 ,ProcWaitFsgWrite
 ,ProcWaitNetThrottle
 ,ProcWaitMonitor
 ,ProcWaitMonResume
 ,ProcWaitDBLock
 ,ProcWaitSegLock
 ,ProcWaitFsgLock
 ,ProcWaitQnl
 ,CPUIdle
 ,CPUIoWait
 ,CPUUServ
 ,CPUUExec
 ,CPUProcSwitches
 ,MemVprAllocKB
 ,MemTextPageReads
 ,MemCtxtPageWrites
 ,MemCtxtPageReads
 ,MsgPtPReads
 ,MsgPtPWrites
 ,MsgBrdReads
 ,MsgBrdWrites
 ,NetTxRouting
 ,NetTxConnected
 ,NetRxConnected
 ,NetTxIdle
 ,NetRxIdle
 ,NetSamples
 ,NetMsgPtPWrites
 ,NetMsgBrdWrites
 ,NetMsgPtPReads
 ,NetMsgBrdReads
 ,NetMrgBlock
 ,NetMsgChannelBlock
 ,NetMsgGroupBlock
 ,NetMsgResourceBlock
 ,NetMsgFCBlock
 ,NetMsgRxBlock
 ,NetBlockQueueTotal
 ,NetBlockQueueMax
 ,NetBlockQueue
 ,NetTxCircHPBrd
 ,NetRxCircPtP
 ,NetTxCircHPPtP
 ,NetRxCircBrd
 ,NetTxCircBrd
 ,NetBackoffs
 ,NetSemInUseMax
 ,NetChanInUseMax
 ,NetGroupInUseMax
 ,NetHWBackoffs
 ,NetTxCircPtP
 ,HostBlockReads
 ,HostBlockWrites
 ,HostMessageReads
 ,HostMessageWrites
 ,DBLockBlocks
 ,DBLockDeadlocks
 ,FileAcqs
 ,FileAcqsOther
 ,FileAcqReads
 ,FileRels
 ,FileRelsOther
 ,FileWrites
 ,FilePres
 ,FilePreReads
 ,FileLockBlocks
 ,FileLockDeadlocks
 ,FileLockEnters
 ,FileSmallDepotWrites
 ,FileLargeDepotWrites
 ,FileLargeDepotBlocks
 ,MsgChnLastDone
 ,CmdDDLStmts
 ,CmdDeleteStmts
 ,CmdInsertStmts
 ,CmdSelectStmts
 ,CmdUpdateStmts
 ,CmdUtilityStmts
 ,CmdOtherStmts
 ,CmdStmtSuccesses
 ,CmdStmtFailures
 ,CmdStmtErrors
 ,AmpsFlowControlled
 ,FlowCtlCnt
 ,AwtInuse
 ,AwtInuseMax
 ,PSNumRequests
 ,TvsReadRespMax
 ,TvsWriteRespMax
 ,NLBActiveSessionsMax
 ,NLBSessionsInuse
 ,NLBSessionsCompleted
 ,NLBMsgFlowControlled
 ,NetActiveMrg
 ,NetMrgCompleted
 ,FileSmallDepotBusy
 ,FileLargeDepotBusy
 ,SegMaxAvailMB
 ,SegInuseMB
 ,SegCacheMB
 ,SegMDLAlloc
 ,SegMDLFree
 ,SegMDLRelease
 ,SegMDLRecycle
 ,SegMDLAllocKB
 ,SegMDLFreeKB
 ,SegMDLReleaseKB
 ,SegMDLRecycleKB
 ,RTCpuTime
 ,RTTasksMax
 ,TDEnabledCPUs
 ,SpareInt
from {resusagespma} -- pdcrinfo.resusageSPMA_hst or dbc.resusageSPMA
where TheDate between {startdate} and {enddate}
;