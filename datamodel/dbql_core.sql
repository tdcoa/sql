CREATE MULTISET GLOBAL TEMPORARY TABLE adlste_coa.stg_dat_DBQL_Core
 (SiteID                           VARCHAR(64)  CHARACTER SET LATIN
 ,LogDate                          DATE
 ,LogHour                          INTEGER
 ,App_Bucket                       VARCHAR(256) CHARACTER SET LATIN
 ,Use_Bucket                       VARCHAR(256) CHARACTER SET LATIN
 ,Statement_Bucket                 VARCHAR(256) CHARACTER SET LATIN
 ,User_Bucket                      VARCHAR(256) CHARACTER SET LATIN
 ,Is_Discrete_Human                CHAR(3)      CHARACTER SET LATIN
 ,User_Department                  VARCHAR(256) CHARACTER SET LATIN
 ,User_SubDepartment               VARCHAR(256) CHARACTER SET LATIN
 ,User_Region                      VARCHAR(256) CHARACTER SET LATIN
 ,WDName                           VARCHAR(256) CHARACTER SET LATIN
 ,Query_Type                       VARCHAR(256) CHARACTER SET LATIN
 ,Total_AMPs                       INTEGER
 ,Query_Cnt                        BIGINT
 ,Query_Error_Cnt                  BIGINT
 ,Query_Abort_Cnt                  BIGINT
 ,Query_NoIO_cnt                   BIGINT
 ,Query_InMem_Cnt                  BIGINT
 ,Query_PhysIO_Cnt                 BIGINT
 ,Request_Cnt                      BIGINT
 ,Query_Complexity_Score_Avg       DECIMAL(18,4)
 ,Returned_Row_Cnt                 BIGINT
 ,DelayTime_Sec                    DECIMAL(18,4)
 ,Runtime_Parse_Sec                DECIMAL(18,4)
 ,Runtime_AMP_Sec                  DECIMAL(18,4)
 ,Runtime_Total_Sec                DECIMAL(18,4)
 ,TransferTime_Sec                 DECIMAL(18,4)
 ,CPU_Parse_Sec                    DECIMAL(18,4)
 ,CPU_AMP_Sec                      DECIMAL(18,4)
 ,IOCntM_Physical                  BIGINT
 ,IOCntM_Total                     BIGINT
 ,IOGB_Physical                    DECIMAL(18,4)
 ,IOGB_Total                       DECIMAL(18,4)
 ,IOTA_Used_cntB                   DECIMAL(18,4)
 ,IOTA_SysMax_cntB                 DECIMAL(18,0)
 ,NumOfActiveAMPs_Avg              DECIMAL(9,4)
 ,Spool_GB                         DECIMAL(18,4)
 ,Spool_GB_Avg                     DECIMAL(9,4)
 ,CPUSec_Skew_AvgPCt               DECIMAL(9,4)
 ,IOCnt_Skew_AvgPct                DECIMAL(9,4)
 ,VeryHot_IOcnt_Cache_AvgPct       DECIMAL(9,4)
 ,VeryHot_IOGB_Cache_AvgPct        DECIMAL(9,4)
 ,CacheMiss_IOPSScore              DECIMAL(9,4)
 ,CacheMiss_KBScore                DECIMAL(9,4)
 ,MultiStatement_Count             INTEGER
 ,MultiStatement_Delete            INTEGER
 ,MultiStatement_Insert            INTEGER
 ,MultiStatement_InsertSel         INTEGER
 ,MultiStatement_Update            INTEGER
 ,MultiStatement_Select            INTEGER
 ,qrycnt_in_runtime_0000_0001      BIGINT
 ,qrycnt_in_runtime_0001_0005      BIGINT
 ,qrycnt_in_runtime_0005_0010      BIGINT
 ,qrycnt_in_runtime_0010_0030      BIGINT
 ,qrycnt_in_runtime_0030_0060      BIGINT
 ,qrycnt_in_runtime_0060_0300      BIGINT
 ,qrycnt_in_runtime_0300_0600      BIGINT
 ,qrycnt_in_runtime_0600_1800      BIGINT
 ,qrycnt_in_runtime_1800_3600      BIGINT
 ,qrycnt_in_runtime_3600_plus      BIGINT
 ,cpusec_in_runtime_0000_0001      DECIMAL(18,4)
 ,cpusec_in_runtime_0001_0005      DECIMAL(18,4)
 ,cpusec_in_runtime_0005_0010      DECIMAL(18,4)
 ,cpusec_in_runtime_0010_0030      DECIMAL(18,4)
 ,cpusec_in_runtime_0030_0060      DECIMAL(18,4)
 ,cpusec_in_runtime_0060_0300      DECIMAL(18,4)
 ,cpusec_in_runtime_0300_0600      DECIMAL(18,4)
 ,cpusec_in_runtime_0600_1800      DECIMAL(18,4)
 ,cpusec_in_runtime_1800_3600      DECIMAL(18,4)
 ,cpusec_in_runtime_3600_plus      DECIMAL(18,4)
 ,iogb_in_runtime_0000_0001        DECIMAL(18,4)
 ,iogb_in_runtime_0001_0005        DECIMAL(18,4)
 ,iogb_in_runtime_0005_0010        DECIMAL(18,4)
 ,iogb_in_runtime_0010_0030        DECIMAL(18,4)
 ,iogb_in_runtime_0030_0060        DECIMAL(18,4)
 ,iogb_in_runtime_0060_0300        DECIMAL(18,4)
 ,iogb_in_runtime_0300_0600        DECIMAL(18,4)
 ,iogb_in_runtime_0600_1800        DECIMAL(18,4)
 ,iogb_in_runtime_1800_3600        DECIMAL(18,4)
 ,iogb_in_runtime_3600_plus        DECIMAL(18,4)
 ,qrycnt_in_delaytime_0000_0001    BIGINT
 ,qrycnt_in_delaytime_0001_0005    BIGINT
 ,qrycnt_in_delaytime_0005_0010    BIGINT
 ,qrycnt_in_delaytime_0010_0030    BIGINT
 ,qrycnt_in_delaytime_0030_0060    BIGINT
 ,qrycnt_in_delaytime_0060_0300    BIGINT
 ,qrycnt_in_delaytime_0300_0600    BIGINT
 ,qrycnt_in_delaytime_0600_1800    BIGINT
 ,qrycnt_in_delaytime_1800_3600    BIGINT
 ,qrycnt_in_delaytime_3600_plus    BIGINT
 ,cpusec_in_delaytime_0000_0001    DECIMAL(18,4)
 ,cpusec_in_delaytime_0001_0005    DECIMAL(18,4)
 ,cpusec_in_delaytime_0005_0010    DECIMAL(18,4)
 ,cpusec_in_delaytime_0010_0030    DECIMAL(18,4)
 ,cpusec_in_delaytime_0030_0060    DECIMAL(18,4)
 ,cpusec_in_delaytime_0060_0300    DECIMAL(18,4)
 ,cpusec_in_delaytime_0300_0600    DECIMAL(18,4)
 ,cpusec_in_delaytime_0600_1800    DECIMAL(18,4)
 ,cpusec_in_delaytime_1800_3600    DECIMAL(18,4)
 ,cpusec_in_delaytime_3600_plus    DECIMAL(18,4)
 ,iogb_in_delaytime_0000_0001      DECIMAL(18,4)
 ,iogb_in_delaytime_0001_0005      DECIMAL(18,4)
 ,iogb_in_delaytime_0005_0010      DECIMAL(18,4)
 ,iogb_in_delaytime_0010_0030      DECIMAL(18,4)
 ,iogb_in_delaytime_0030_0060      DECIMAL(18,4)
 ,iogb_in_delaytime_0060_0300      DECIMAL(18,4)
 ,iogb_in_delaytime_0300_0600      DECIMAL(18,4)
 ,iogb_in_delaytime_0600_1800      DECIMAL(18,4)
 ,iogb_in_delaytime_1800_3600      DECIMAL(18,4)
 ,iogb_in_delaytime_3600_plus      DECIMAL(18,4)
 ) No Primary index
 on commit preserve rows;



 CREATE MULTISET TABLE adlste_coa.coat_dat_DBQL_Core
  (SiteID                           VARCHAR(64)  CHARACTER SET LATIN
  ,LogDate                          DATE
  ,LogHour                          INTEGER
  ,App_Bucket                       VARCHAR(256) CHARACTER SET LATIN
  ,Use_Bucket                       VARCHAR(256) CHARACTER SET LATIN
  ,Statement_Bucket                 VARCHAR(256) CHARACTER SET LATIN
  ,User_Bucket                      VARCHAR(256) CHARACTER SET LATIN
  ,Is_Discrete_Human                CHAR(3)      CHARACTER SET LATIN
  ,User_Department                  VARCHAR(256) CHARACTER SET LATIN
  ,User_SubDepartment               VARCHAR(256) CHARACTER SET LATIN
  ,User_Region                      VARCHAR(256) CHARACTER SET LATIN
  ,WDName                           VARCHAR(256) CHARACTER SET LATIN
  ,Query_Type                       VARCHAR(256) CHARACTER SET LATIN
  ,Total_AMPs                       INTEGER
  ,Query_Cnt                        BIGINT
  ,Query_Error_Cnt                  BIGINT
  ,Query_Abort_Cnt                  BIGINT
  ,Query_NoIO_cnt                   BIGINT
  ,Query_InMem_Cnt                  BIGINT
  ,Query_PhysIO_Cnt                 BIGINT
  ,Request_Cnt                      BIGINT
  ,Query_Complexity_Score_Avg       DECIMAL(18,4)
  ,Returned_Row_Cnt                 BIGINT
  ,DelayTime_Sec                    DECIMAL(18,4)
  ,Runtime_Parse_Sec                DECIMAL(18,4)
  ,Runtime_AMP_Sec                  DECIMAL(18,4)
  ,Runtime_Total_Sec                DECIMAL(18,4)
  ,TransferTime_Sec                 DECIMAL(18,4)
  ,CPU_Parse_Sec                    DECIMAL(18,4)
  ,CPU_AMP_Sec                      DECIMAL(18,4)
  ,IOCntM_Physical                  BIGINT
  ,IOCntM_Total                     BIGINT
  ,IOGB_Physical                    DECIMAL(18,4)
  ,IOGB_Total                       DECIMAL(18,4)
  ,IOTA_Used_cntB                   DECIMAL(18,4)
  ,IOTA_SysMax_cntB                 DECIMAL(18,0)
  ,NumOfActiveAMPs_Avg              DECIMAL(9,4)
  ,Spool_GB                         DECIMAL(18,4)
  ,Spool_GB_Avg                     DECIMAL(9,4)
  ,CPUSec_Skew_AvgPCt               DECIMAL(9,4)
  ,IOCnt_Skew_AvgPct                DECIMAL(9,4)
  ,VeryHot_IOcnt_Cache_AvgPct       DECIMAL(9,4)
  ,VeryHot_IOGB_Cache_AvgPct        DECIMAL(9,4)
  ,CacheMiss_IOPSScore              DECIMAL(9,4)
  ,CacheMiss_KBScore                DECIMAL(9,4)
  ,MultiStatement_Count             INTEGER
  ,MultiStatement_Delete            INTEGER
  ,MultiStatement_Insert            INTEGER
  ,MultiStatement_InsertSel         INTEGER
  ,MultiStatement_Update            INTEGER
  ,MultiStatement_Select            INTEGER
  ,qrycnt_in_runtime_0000_0001      BIGINT
  ,qrycnt_in_runtime_0001_0005      BIGINT
  ,qrycnt_in_runtime_0005_0010      BIGINT
  ,qrycnt_in_runtime_0010_0030      BIGINT
  ,qrycnt_in_runtime_0030_0060      BIGINT
  ,qrycnt_in_runtime_0060_0300      BIGINT
  ,qrycnt_in_runtime_0300_0600      BIGINT
  ,qrycnt_in_runtime_0600_1800      BIGINT
  ,qrycnt_in_runtime_1800_3600      BIGINT
  ,qrycnt_in_runtime_3600_plus      BIGINT
  ,cpusec_in_runtime_0000_0001      DECIMAL(18,4)
  ,cpusec_in_runtime_0001_0005      DECIMAL(18,4)
  ,cpusec_in_runtime_0005_0010      DECIMAL(18,4)
  ,cpusec_in_runtime_0010_0030      DECIMAL(18,4)
  ,cpusec_in_runtime_0030_0060      DECIMAL(18,4)
  ,cpusec_in_runtime_0060_0300      DECIMAL(18,4)
  ,cpusec_in_runtime_0300_0600      DECIMAL(18,4)
  ,cpusec_in_runtime_0600_1800      DECIMAL(18,4)
  ,cpusec_in_runtime_1800_3600      DECIMAL(18,4)
  ,cpusec_in_runtime_3600_plus      DECIMAL(18,4)
  ,iogb_in_runtime_0000_0001        DECIMAL(18,4)
  ,iogb_in_runtime_0001_0005        DECIMAL(18,4)
  ,iogb_in_runtime_0005_0010        DECIMAL(18,4)
  ,iogb_in_runtime_0010_0030        DECIMAL(18,4)
  ,iogb_in_runtime_0030_0060        DECIMAL(18,4)
  ,iogb_in_runtime_0060_0300        DECIMAL(18,4)
  ,iogb_in_runtime_0300_0600        DECIMAL(18,4)
  ,iogb_in_runtime_0600_1800        DECIMAL(18,4)
  ,iogb_in_runtime_1800_3600        DECIMAL(18,4)
  ,iogb_in_runtime_3600_plus        DECIMAL(18,4)
  ,qrycnt_in_delaytime_0000_0001    BIGINT
  ,qrycnt_in_delaytime_0001_0005    BIGINT
  ,qrycnt_in_delaytime_0005_0010    BIGINT
  ,qrycnt_in_delaytime_0010_0030    BIGINT
  ,qrycnt_in_delaytime_0030_0060    BIGINT
  ,qrycnt_in_delaytime_0060_0300    BIGINT
  ,qrycnt_in_delaytime_0300_0600    BIGINT
  ,qrycnt_in_delaytime_0600_1800    BIGINT
  ,qrycnt_in_delaytime_1800_3600    BIGINT
  ,qrycnt_in_delaytime_3600_plus    BIGINT
  ,cpusec_in_delaytime_0000_0001    DECIMAL(18,4)
  ,cpusec_in_delaytime_0001_0005    DECIMAL(18,4)
  ,cpusec_in_delaytime_0005_0010    DECIMAL(18,4)
  ,cpusec_in_delaytime_0010_0030    DECIMAL(18,4)
  ,cpusec_in_delaytime_0030_0060    DECIMAL(18,4)
  ,cpusec_in_delaytime_0060_0300    DECIMAL(18,4)
  ,cpusec_in_delaytime_0300_0600    DECIMAL(18,4)
  ,cpusec_in_delaytime_0600_1800    DECIMAL(18,4)
  ,cpusec_in_delaytime_1800_3600    DECIMAL(18,4)
  ,cpusec_in_delaytime_3600_plus    DECIMAL(18,4)
  ,iogb_in_delaytime_0000_0001      DECIMAL(18,4)
  ,iogb_in_delaytime_0001_0005      DECIMAL(18,4)
  ,iogb_in_delaytime_0005_0010      DECIMAL(18,4)
  ,iogb_in_delaytime_0010_0030      DECIMAL(18,4)
  ,iogb_in_delaytime_0030_0060      DECIMAL(18,4)
  ,iogb_in_delaytime_0060_0300      DECIMAL(18,4)
  ,iogb_in_delaytime_0300_0600      DECIMAL(18,4)
  ,iogb_in_delaytime_0600_1800      DECIMAL(18,4)
  ,iogb_in_delaytime_1800_3600      DECIMAL(18,4)
  ,iogb_in_delaytime_3600_plus      DECIMAL(18,4)
) Primary index(LogDate, SiteID)
  Partition by (
    COLUMN(SiteID)
    ,RANGE_N (LogDate BETWEEN DATE '2019-01-01'
                          AND DATE '2021-12-31'
                      EACH INTERVAL '1' DAY
              ,NO RANGE OR UNKNOWN)
    );


REPLACE PROCEDURE adlste_coa.sp_dat_DBQL_Core ( fileset_version VARCHAR(128) )
BEGIN

  delete from adlste_coa.coat_dat_DBQL_Core
  where   (SiteID, LogDate) in
   (Select SiteID, LogDate From adlste_coa.stg_dat_DBQL_Core);

  Insert into adlste_coa.coat_dat_DBQL_Core
  Select * From adlste_coa.stg_dat_DBQL_Core;
END;


Replace View adlste_coa.COA_dat_DBQL_Core as
locking row for access
Select * from adlste_coa.coat_dat_DBQL_Core;
