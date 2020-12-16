/* DBQL_CORE -- default SELECT statements that return rows from volatile tables.
   This is broken out so it can be overridden by other processes, as needed.

   Parameters:  none beyond normal config.yaml items...
                dates are constrained during CVT
*/


/*{{save:Query_Breakouts.csv}}*/
/*{{load:{db_stg}.stg_dat_DBQL_Core_QryCnt_Ranges}}*/
/*{{call:{db_coa}.sp_dat_DBQL_Core_QryCnt_Ranges('{fileset_version}')}}*/
Select  /*dbql_core*/
   '{siteid}'  as Site_ID
  ,cast(LogDate as format 'Y4-MM-DD')(varchar(128)) as LogDate
  ,usr.User_Bucket
  ,usr.User_Department
  ,usr.User_SubDepartment
  ,cast(qrycnt_in_runtime_0000_0001   as decimal(18,2))   as qrycnt_in_runtime_0000_0001
  ,cast(qrycnt_in_runtime_0001_0005   as decimal(18,2))   as qrycnt_in_runtime_0001_0005
  ,cast(qrycnt_in_runtime_0005_0010   as decimal(18,2))   as qrycnt_in_runtime_0005_0010
  ,cast(qrycnt_in_runtime_0010_0030   as decimal(18,2))   as qrycnt_in_runtime_0010_0030
  ,cast(qrycnt_in_runtime_0030_0060   as decimal(18,2))   as qrycnt_in_runtime_0030_0060
  ,cast(qrycnt_in_runtime_0060_0300   as decimal(18,2))   as qrycnt_in_runtime_0060_0300
  ,cast(qrycnt_in_runtime_0300_0600   as decimal(18,2))   as qrycnt_in_runtime_0300_0600
  ,cast(qrycnt_in_runtime_0600_1800   as decimal(18,2))   as qrycnt_in_runtime_0600_1800
  ,cast(qrycnt_in_runtime_1800_3600   as decimal(18,2))   as qrycnt_in_runtime_1800_3600
  ,cast(qrycnt_in_runtime_3600_plus   as decimal(18,2))   as qrycnt_in_runtime_3600_plus
  ,cast(cpusec_in_runtime_0000_0001   as decimal(18,2)) as cpusec_in_runtime_0000_0001
  ,cast(cpusec_in_runtime_0001_0005   as decimal(18,2)) as cpusec_in_runtime_0001_0005
  ,cast(cpusec_in_runtime_0005_0010   as decimal(18,2)) as cpusec_in_runtime_0005_0010
  ,cast(cpusec_in_runtime_0010_0030   as decimal(18,2)) as cpusec_in_runtime_0010_0030
  ,cast(cpusec_in_runtime_0030_0060   as decimal(18,2)) as cpusec_in_runtime_0030_0060
  ,cast(cpusec_in_runtime_0060_0300   as decimal(18,2)) as cpusec_in_runtime_0060_0300
  ,cast(cpusec_in_runtime_0300_0600   as decimal(18,2)) as cpusec_in_runtime_0300_0600
  ,cast(cpusec_in_runtime_0600_1800   as decimal(18,2)) as cpusec_in_runtime_0600_1800
  ,cast(cpusec_in_runtime_1800_3600   as decimal(18,2)) as cpusec_in_runtime_1800_3600
  ,cast(cpusec_in_runtime_3600_plus   as decimal(18,2)) as cpusec_in_runtime_3600_plus
  ,cast(iogb_in_runtime_0000_0001     as decimal(18,2)) as iogb_in_runtime_0000_0001
  ,cast(iogb_in_runtime_0001_0005     as decimal(18,2)) as iogb_in_runtime_0001_0005
  ,cast(iogb_in_runtime_0005_0010     as decimal(18,2)) as iogb_in_runtime_0005_0010
  ,cast(iogb_in_runtime_0010_0030     as decimal(18,2)) as iogb_in_runtime_0010_0030
  ,cast(iogb_in_runtime_0030_0060     as decimal(18,2)) as iogb_in_runtime_0030_0060
  ,cast(iogb_in_runtime_0060_0300     as decimal(18,2)) as iogb_in_runtime_0060_0300
  ,cast(iogb_in_runtime_0300_0600     as decimal(18,2)) as iogb_in_runtime_0300_0600
  ,cast(iogb_in_runtime_0600_1800     as decimal(18,2)) as iogb_in_runtime_0600_1800
  ,cast(iogb_in_runtime_1800_3600     as decimal(18,2)) as iogb_in_runtime_1800_3600
  ,cast(iogb_in_runtime_3600_plus     as decimal(18,2)) as iogb_in_runtime_3600_plus
  ,cast(qrycnt_in_delaytime_0000_0001 as decimal(18,2)) as qrycnt_in_delaytime_0000_0001
  ,cast(qrycnt_in_delaytime_0001_0005 as decimal(18,2)) as qrycnt_in_delaytime_0001_0005
  ,cast(qrycnt_in_delaytime_0005_0010 as decimal(18,2)) as qrycnt_in_delaytime_0005_0010
  ,cast(qrycnt_in_delaytime_0010_0030 as decimal(18,2)) as qrycnt_in_delaytime_0010_0030
  ,cast(qrycnt_in_delaytime_0030_0060 as decimal(18,2)) as qrycnt_in_delaytime_0030_0060
  ,cast(qrycnt_in_delaytime_0060_0300 as decimal(18,2)) as qrycnt_in_delaytime_0060_0300
  ,cast(qrycnt_in_delaytime_0300_0600 as decimal(18,2)) as qrycnt_in_delaytime_0300_0600
  ,cast(qrycnt_in_delaytime_0600_1800 as decimal(18,2)) as qrycnt_in_delaytime_0600_1800
  ,cast(qrycnt_in_delaytime_1800_3600 as decimal(18,2)) as qrycnt_in_delaytime_1800_3600
  ,cast(qrycnt_in_delaytime_3600_plus as decimal(18,2)) as qrycnt_in_delaytime_3600_plus
  ,cast(cpusec_in_delaytime_0000_0001 as decimal(18,2)) as cpusec_in_delaytime_0000_0001
  ,cast(cpusec_in_delaytime_0001_0005 as decimal(18,2)) as cpusec_in_delaytime_0001_0005
  ,cast(cpusec_in_delaytime_0005_0010 as decimal(18,2)) as cpusec_in_delaytime_0005_0010
  ,cast(cpusec_in_delaytime_0010_0030 as decimal(18,2)) as cpusec_in_delaytime_0010_0030
  ,cast(cpusec_in_delaytime_0030_0060 as decimal(18,2)) as cpusec_in_delaytime_0030_0060
  ,cast(cpusec_in_delaytime_0060_0300 as decimal(18,2)) as cpusec_in_delaytime_0060_0300
  ,cast(cpusec_in_delaytime_0300_0600 as decimal(18,2)) as cpusec_in_delaytime_0300_0600
  ,cast(cpusec_in_delaytime_0600_1800 as decimal(18,2)) as cpusec_in_delaytime_0600_1800
  ,cast(cpusec_in_delaytime_1800_3600 as decimal(18,2)) as cpusec_in_delaytime_1800_3600
  ,cast(cpusec_in_delaytime_3600_plus as decimal(18,2)) as cpusec_in_delaytime_3600_plus
  ,cast(iogb_in_delaytime_0000_0001   as decimal(18,2)) as iogb_in_delaytime_0000_0001
  ,cast(iogb_in_delaytime_0001_0005   as decimal(18,2)) as iogb_in_delaytime_0001_0005
  ,cast(iogb_in_delaytime_0005_0010   as decimal(18,2)) as iogb_in_delaytime_0005_0010
  ,cast(iogb_in_delaytime_0010_0030   as decimal(18,2)) as iogb_in_delaytime_0010_0030
  ,cast(iogb_in_delaytime_0030_0060   as decimal(18,2)) as iogb_in_delaytime_0030_0060
  ,cast(iogb_in_delaytime_0060_0300   as decimal(18,2)) as iogb_in_delaytime_0060_0300
  ,cast(iogb_in_delaytime_0300_0600   as decimal(18,2)) as iogb_in_delaytime_0300_0600
  ,cast(iogb_in_delaytime_0600_1800   as decimal(18,2)) as iogb_in_delaytime_0600_1800
  ,cast(iogb_in_delaytime_1800_3600   as decimal(18,2)) as iogb_in_delaytime_1800_3600
  ,cast(iogb_in_delaytime_3600_plus   as decimal(18,2)) as iogb_in_delaytime_3600_plus
from dbql_core_breakout;


/*{{save:cpu_summary.csv}}*/
/*{{load:{db_stg}.stg_dat_dbql_core_maxcpu}}*/
/*{{call:{db_coa}.sp_dat_dbql_core_maxcpu('{fileset_version}')}}*/
Select
'{siteid}' as Site_ID
,cast(LogDate as DATE format'yyyy-mm-dd')(char(10)) as LogDate
,LogHour
,UTC_Offset
,Node_Type
,Node_Cnt
,vCPU_per_Node
,MaxIOTA_cntB
,CPU_Idle
,CPU_IOWait
,CPU_OS
,CPU_DBS
,CPU_Total
from cpu_summary_hourly
;


/*{{save:DBQL_Core.csv}}*/
/*{{load:{db_stg}.stg_dat_DBQL_Core}}*/
/*{{call:{db_coa}.sp_dat_DBQL_Core('{fileset_version}')}}*/
SELECT
 '{siteid}'  as Site_ID
,LogTS
,max(Total_AMPs) as Total_AMPs
,app.App_Bucket
,app.Use_Bucket
,stm.Statement_Bucket
,usr.User_Bucket
,usr.User_Department
,usr.User_SubDepartment
,sum(Request_Cnt                ) as Request_Cnt
,sum(Query_Cnt                  ) as Query_Cnt
,sum(Query_MultiStatement_Cnt   ) as Query_MultiStatement_Cnt
,sum(Query_Error_Cnt            ) as Query_Error_Cnt
,sum(Query_Abort_Cnt            ) as Query_Abort_Cnt
,sum(Query_NoIO_cnt             ) as Query_NoIO_cnt
,sum(Query_InMem_Cnt            ) as Query_InMem_Cnt
,sum(Query_PhysIO_Cnt           ) as Query_PhysIO_Cnt
,sum(Query_Tactical_Cnt         ) as Query_Tactical_Cnt
,avg(Query_Complexity_Score_Avg ) as Query_Complexity_Score_Avg
,sum(Returned_Row_Cnt           ) as Returned_Row_Cnt
,sum(DelayTime_Sec              ) as DelayTime_Sec
,sum(RunTime_Parse_Sec          ) as RunTime_Parse_Sec
,sum(Runtime_AMP_Sec            ) as Runtime_AMP_Sec
,sum(RunTime_Total_Sec          ) as RunTime_Total_Sec
,sum(TransferTime_Sec           ) as TransferTime_Sec
,sum(CPU_Parse_Sec              ) as CPU_Parse_Sec
,sum(CPU_AMP_Sec                ) as CPU_AMP_Sec
,sum(IOCntM_Physical            ) as IOCntM_Physical
,sum(IOCntM_Total               ) as IOCntM_Total
,sum(IOGB_Physical              ) as IOGB_Physical
,sum(IOGB_Total                 ) as IOGB_Total
,sum(IOTA_Used_cntB             ) as IOTA_Used_cntB
,avg(NumOfActiveAMPs_Avg        ) as NumOfActiveAMPs_Avg
,sum(Spool_GB                   ) as Spool_GB
,avg(CacheHit_Pct               ) as CacheHit_Pct
,avg(CPUSec_Skew_AvgPCt         ) as CPUSec_Skew_AvgPCt
,avg(IOCnt_Skew_AvgPct          ) as IOCnt_Skew_AvgPct
from dbql_core_hourly dbql
join dim_app as app     on dbql.AppID = app.AppID
join dim_Statement stm  on dbql.StatementType = stm.StatementType
join dim_user usr       on dbql.UserName = usr.UserName
Group by
 Site_ID
,LogTS
,app.App_Bucket
,app.Use_Bucket
,stm.Statement_Bucket
,usr.User_Bucket
,usr.User_Department
,usr.User_SubDepartment
;
