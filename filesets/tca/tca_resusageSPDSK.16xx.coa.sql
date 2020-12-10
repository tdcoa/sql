/* TCA Process for ResUsageSPDSK == 1620 */ 
/*  Parameters:
     startdate = {startdate}
     enddate = {enddate}
     resusagespdsk = {resusagespdsk}
     dbprefix = {dbprefix} <-- probably blank, but set to "Dev_" in the FileSet during TCA Testing
     dbsversion_label = {dbsversion_label}
 */
 
/*{{save:tca_resusageSVPR_{dbsversion_label}.csv}}*/
/*{{load:{dbprefix}APP_TCA_TMP.stg_tca_resusageSPDSK_{dbsversion_label}}}*/
/*{{call:{dbprefix}APP_TCA_TBL.sp_tca_resusageSPDSK_{dbsversion_label}()}}*/
Select 
  Site_ID
 ,TheDate
 ,NodeID
 ,TheTime
 ,GmtTime
 ,NodeType
 ,CabinetID
 ,ModuleID
 ,TheTimestamp
 ,CentiSecs
 ,Secs
 ,NominalSecs
 ,SummaryFlag
 ,Reserved
 ,PM_COD_CPU
 ,PM_COD_IO
 ,WM_COD_CPU
 ,WM_COD_IO
 ,TIER_FACTOR
 ,PdiskGlobalId
 ,PdiskDeviceId
 ,PdiskType
 ,Reserved0
 ,ReadKB
 ,WriteKB
 ,ReadRespTot
 ,WriteRespTot
 ,ReadRespSq
 ,WriteRespSq
 ,ExtMigrateReadRespTot
 ,ExtMigrateWriteRespTot
 ,ExtMigrateIOTimeImprove
 ,DiskCacheReadCnt
 ,DiskCacheWriteCnt
 ,DiskCacheReadHitCnt
 ,DiskCacheWriteHitCnt
 ,DiskCacheFillCnt
 ,DiskCacheInUseKB
 ,DiskCacheTotalKB
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
 ,ReadCnt
 ,WriteCnt
 ,ReadRespMax
 ,WriteRespMax
 ,ConcurrentMax
 ,ConcurrentReadMax
 ,ConcurrentWriteMax
 ,MigrationBlockedIos
 ,ExtAllocHot
 ,ExtAllocWarm
 ,ExtAllocTotal
 ,ExtAllocNonPacing
 ,ExtAllocSystemPacing
 ,ExtAllocStatic
 ,ExtMigrateFaster
 ,ExtMigrateTotal
 ,ExtMigrateIOTimeCost
 ,ExtMigrateIOTimeBenefit
 ,SpareInt
from {resusagespdsk} -- pdcrinfo.resusageSPDSK_hst or dbc.resusageSPDSK
where TheDate between {startdate} and {enddate}
;