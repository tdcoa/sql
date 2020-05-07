/* Pulls all DBQL Data Required for CSM - CONSUMPTION ANALYTICS (COA)
   see comments about each SQL step inline below.

Parameters:
  - startdate:    {startdate}
  - enddate:      {enddate}
  - siteid:       {siteid}
  - dbqlogtbl:    {dbqlogtbl}
  - resusagespma: {resusagespma}
*/



/*{{temp:dim_app.csv}}*/
create volatile table dim_app as
(
  select
   o.AppID
  ,coalesce(p.App_Bucket,'Unknown') as App_Bucket
  ,coalesce(p.Use_Bucket,'Unknown')  as Use_Bucket
  ,coalesce(p.Priority,1e6) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.AppID)      as Pattern
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (select distinct AppID from {dbqlogtbl}
        where LogDate between {startdate} and {enddate}) as o
  left join "dim_app.csv" as p
    on (case
        when p.Pattern_Type = 'Equal' and o.AppID = p.Pattern then 1
        when p.Pattern_Type = 'Like'  and o.AppID like p.Pattern then 1
        when p.Pattern_Type = 'RegEx'
         and character_length(regexp_substr(o.AppID, p.Pattern,1,1,'i'))>0 then 1
        else 0 end) = 1
  qualify Priority_ = min(Priority_)over(partition by o.AppID)
  where SiteID_ in('default','None') or SiteID_ like '{siteid}'
) with data
no primary index
on commit preserve rows;

drop table "dim_app.csv";

/*{{save:dim_app_reconcile.csv}}*/
Select * from dim_App
order by case when  App_Bucket='Unknown' then '!!!' else App_Bucket end asc
;



/*{{temp:dim_statement.csv}}*/
create volatile table dim_statement as
(
  select
   o.StatementType
  ,coalesce(p.Statement_Bucket,'Unknown') as Statement_Bucket
  ,coalesce(p.Priority,1e6) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.StatementType) as Pattern
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (select distinct StatementType from {dbqlogtbl}
        where LogDate between {startdate} and {enddate}) as o
  left join "dim_statement.csv"  as p
    on (case
        when p.Pattern_Type = 'Equal' and o.StatementType = p.Pattern then 1
        when p.Pattern_Type = 'Like'  and o.StatementType like p.Pattern then 1
        when p.Pattern_Type = 'RegEx'
         and character_length(regexp_substr(o.StatementType, p.Pattern,1,1,'i'))>0 then 1
        else 0 end) = 1
  qualify Priority_ = min(Priority_)over(partition by o.StatementType)
  where SiteID_ in('default','None') or SiteID_ like '{siteid}'
) with data
no primary index
on commit preserve rows;

drop table "dim_statement.csv";

/*{{save:dim_statement_reconcile.csv}}*/
Select * from dim_statement
order by case when  Statement_Bucket='Unknown' then '!!!' else Statement_Bucket end asc



/*{{temp:dim_user.csv}}*/
;

/* below override sql file allows opportunity to
   replace dim_user.csv with ca_user_xref table
   or a customer-specific table.  To use, review
   and fill-in the .sql file content:
*/
/*{{file:override_user_dim.sql}}*/
;


create volatile table dim_user as
(
  select
   trim(o.UserName) as Username
  ,coalesce(p.User_Bucket,'Unknown') as User_Bucket
  ,coalesce(p.Is_Discrete_Human,'Unk') as Is_Discrete_Human
  ,coalesce(p.User_Department, 'Unknown') as User_Department
  ,coalesce(p.User_SubDepartment, 'Unknown') as User_SubDepartment
  ,coalesce(p.User_Region, 'Unknown') as User_Region
  ,coalesce(p.Priority,1e6) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.UserName) as Pattern
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (select distinct UserName from {dbqlogtbl}
        where LogDate between {startdate} and {enddate}) as o
  left join "dim_user.csv" as p
    on (case
        when p.Pattern_Type = 'Equal' and o.UserName = p.Pattern then 1
        when p.Pattern_Type = 'Like'  and o.UserName like p.Pattern then 1
        when p.Pattern_Type = 'RegEx'
         and character_length(regexp_substr(o.UserName, p.Pattern,1,1,'i'))>0 then 1
        else 0 end) = 1
  qualify Priority_ = min(Priority_)over(partition by o.UserName)
  where SiteID_ in('default','None') or SiteID_ like '{siteid}'
) with data
no primary index
on commit preserve rows;

drop table "dim_user.csv"
;



/*{{save:dim_user_reconcile.csv}}*/
Select hashrow(substr(Username,1,1))                                     /* first character  */
    || hashrow(substr(Username,floor(character_length(Username)/2)+1,1)) /* middle character */
    || hashrow(substr(Username,character_length(Username),1))            /* last character   */
    || hashrow(Username)                                                 /* entire value */
        as hash_userid
,usr.*
from dim_user as usr
order by case when User_Bucket='Unknown' then '!!!' else User_Bucket end asc
;



/*
 DAT_DBQL  (Final Output)
=========================
Aggregates DBQL into a per-day, per-hour, per-DIM (app/statement/user) buckets
as defined above.  The intention is this to be a smaller table than the
detail, however, this assumption largely relies on how well the bucketing
logic is defined / setup above.  If the result set is too large, try
revisiting the Bucket definitions above, and make groups smaller / less varied.

Also - many compound metrics have been stripped, to minimize transfer file
size.  As these fields are easily calculated, they will be re-constituted
in Transcend.
*/


/*{{save:0001.DBQL_Summary.OUTPUT-{siteid}.csv}}*/
/*{{load:{db_coa}_stg.stg_dat_DBQL_Core}}*/
/*{{call:{db_coa}.sp_dat_dbql_core('{fileset_version}')}}*/
SELECT
 '{siteid}'  as SiteID
 /* TIME Dimension */
,dbql.LogDate
,cast(extract(HOUR from StartTime) as INT format '99') as LogHour
/* ,cast(cast(dbql.LogDate as char(10)) ||' '||
    cast(LogHour as char(2))||':00:00.000000'  as timestamp(6)) as LogTS */

/* all other dimennsions (bucketed for space) */
,app.App_Bucket
,app.Use_Bucket
,stm.Statement_Bucket
,usr.User_Bucket
,usr.Is_Discrete_Human
,usr.User_Department
,usr.User_SubDepartment
,usr.User_Region
,dbql.WDName

,case
 when stm.Statement_Bucket = 'Select'
  and app.App_Bucket not in ('TPT')
  and (ZEROIFNULL( CAST(
     (EXTRACT(HOUR   FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) * 3600)
    +(EXTRACT(MINUTE FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) *   60)
    +(EXTRACT(SECOND FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) *    1)
     as FLOAT))) <= 1  /* Runtime_AMP_Sec */
  and dbql.NumOfActiveAMPs < Total_AMPs
 then 'Tactical'
 else 'Non-Tactical'
/* TODO: Query_Type - design other query types */
 end as Query_Type

/* ====== Query Metrics ======= */
,cast(HashAmp()+1 as Integer) as Total_AMPs
,zeroifnull(sum(cast( dbql.Statements as BigInt))) as Query_Cnt
,zeroifnull(sum(cast( (case when dbql.ErrorCode <> 0                then dbql.Statements else 0 end) as bigint))) as Query_Error_Cnt
,zeroifnull(sum(cast( (case when dbql.Abortflag = 'Y'               then dbql.Statements else 0 end) as bigint))) as Query_Abort_Cnt
,zeroifnull(sum(cast( (case when TotalIOCount = 0                   then dbql.Statements else 0 end) as bigint))) as Query_NoIO_cnt
,zeroifnull(sum(cast( (case when TotalIOCount > 0 AND ReqPhysIO = 0 then dbql.Statements else 0 end) as bigint))) as Query_InMem_Cnt
,zeroifnull(sum(cast( (case when TotalIOCount > 0 AND ReqPhysIO > 0 then dbql.Statements else 0 end) as bigint))) as Query_PhysIO_Cnt

,zeroifnull(cast(count(1) as BigInt)) as Request_Cnt
,zeroifnull(avg(cast(dbql.NumSteps * (character_length(dbql.QueryText)/100) as BigInt) )) as Query_Complexity_Score_Avg
,zeroifnull(sum(cast(dbql.NumResultRows as decimal(18,0)) )) as Returned_Row_Cnt


/* ====== Metrics: RunTimes ====== */
,sum(cast(dbql.DelayTime as decimal(18,2))) as DelayTime_Sec
,sum(ZEROIFNULL(CAST(
   (EXTRACT(HOUR   FROM ((FirstStepTime - StartTime) HOUR(3) TO SECOND(6)) ) * 3600)
  +(EXTRACT(MINUTE FROM ((FirstStepTime - StartTime) HOUR(3) TO SECOND(6)) ) *   60)
  +(EXTRACT(SECOND FROM ((FirstStepTime - StartTime) HOUR(3) TO SECOND(6)) ) *    1)
   as FLOAT))) as Runtime_Parse_Sec
,sum(ZEROIFNULL(CAST(
   (EXTRACT(HOUR   FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) * 3600)
  +(EXTRACT(MINUTE FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) *   60)
  +(EXTRACT(SECOND FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) *    1)
   as FLOAT))) as Runtime_AMP_Sec
,sum(TotalFirstRespTime)  as Runtime_Total_Sec
,sum(ZEROIFNULL(CAST(
   case when LastRespTime is not null then
   (EXTRACT(HOUR   FROM ((LastRespTime - FirstRespTime) HOUR(3) TO SECOND(6)) ) * 3600)
  +(EXTRACT(MINUTE FROM ((LastRespTime - FirstRespTime) HOUR(3) TO SECOND(6)) ) *   60)
  +(EXTRACT(SECOND FROM ((LastRespTime - FirstRespTime) HOUR(3) TO SECOND(6)) ) *    1)
  else 0 end as FLOAT))) AS TransferTime_Sec
/* Runtime_Parse_Sec + Runtime_AMP_Sec = Runtime_Execution_Sec */
/* DelayTime_Sec + Runtime_Execution_Sec + TransferTime_Sec as Runtime_UserExperience_Sec */

/* ====== Metrics: CPU & IO ====== */
,zeroifnull(sum( cast(dbql.ParserCPUTime as decimal(18,2)))) as CPU_Parse_Sec
,zeroifnull(sum( cast(dbql.AMPCPUtime    as decimal(18,2)))) as CPU_AMP_Sec
/* TODO: check if failed queries log CPU consumption */

,zeroifnull(sum( cast(ReqPhysIO/1e6    as decimal(18,0)))) as IOCntM_Physical
,zeroifnull(sum( cast(TotalIOCount/1e6 as decimal(18,0)))) as IOCntM_Total
,zeroifnull(sum( cast(ReqPhysIOKB/1e6  as decimal(18,0)))) as IOGB_Physical
,zeroifnull(sum( cast(ReqIOKB/1e6      as decimal(18,0)))) as IOGB_Total
,zeroifnull(sum( cast(dbql.UsedIOTA/1e9    as decimal(18,4)))) as IOTA_Used_cntB
,zeroifnull(sum( cast(maxiota.MaxIOTA_cntB as decimal(18,0)))) as IOTA_SysMax_cntB

/* ====== Metrics: Other ====== */
,zeroifnull(avg(NumOfActiveAMPs)) as NumOfActiveAMPs_Avg
,zeroifnull(sum(SpoolUsage/1e9)) as Spool_GB
,zeroifnull(avg(SpoolUsage/1e9)) as Spool_GB_Avg

,zeroifnull(avg((AMPCPUTime / nullifzero(MaxAmpCPUTime*NumOfActiveAMPs))-1)) as CPUSec_Skew_AvgPCt
,zeroifnull(avg((TotalIOCount / nullifzero(MaxAmpIO*NumOfActiveAMPs))-1) ) as IOCnt_Skew_AvgPct
,zeroifnull(avg(VHPhysIO / nullifzero(VHLogicalIO))) as VeryHot_IOcnt_Cache_AvgPct
,zeroifnull(avg(VHPhysIOKB / nullifzero(VHLogicalIOKB))) as VeryHot_IOGB_Cache_AvgPct

/* METRIC:   Cache Miss Rate IOPS.  normal cache miss rate <20%,   set score = 0  for  miss rate < 20%,  increments of 10%, range 0 -80, then scaled to 0-100 */
,zeroifnull(avg(case when ReqPhysIO = 0 then 0
          when zeroifnull(ReqPhysIO/ nullifzero(TotalIOCount)) <= 0.20 then 0                              /* set score = 0 when less than industry average 20% */
          when ReqPhysIO > TotalIOCount then 80                                                            /* sometimes get Physical > Logical, set ceiling at 80*/
          else (cast( 100 * zeroifnull (ReqPhysIO/ nullifzero(TotalIOCount)) /10 as  integer) * 10) - 20   /* only count above 20%, round to bin size 10*/
          end) / .8  )  /*  scale up to 0 - 100 */
     as CacheMiss_IOPSScore

/* METRIC:   Cache Miss Rate KB.  normal cache miss rate <20%,   set score = 0  for  miss rate < 20%,  increments of 10%, range 0 -80 */
,zeroifnull(avg(case when  ReqPhysIOKB = 0 then 0
          when   zeroifnull(ReqPhysIOKB/ nullifzero(ReqIOKB)) <= 0.20 then 0                             /* set score = 0 when less than industry average 20% */
          when   ReqPhysIOKB > ReqIOKB then 80                                                           /* sometimes get Physical > Logical, set ceiling at 80*/
          else  (cast( 100 * zeroifnull (ReqPhysIOKB/ nullifzero(ReqIOKB)) /10 as  integer) * 10) - 20   /* only count above 20%, round to bin size 10*/
          end) / .8  )    /*  scale up to 0 - 100 */
     as CacheMiss_KBScore

/* ====== Multi-Statement Break-Out, if interested: ====== */
,count(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end) as MultiStatement_Count
,zeroifnull(sum(cast(trim(
    substr(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Del=')+4,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Ins=')-
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Del=')-4)) as INT))
  ) as MultiStatement_Delete
,zeroifnull(sum(cast(trim(substr(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Ins=')+4,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'InsSel=')-
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Ins=')-4)) as INT))
  ) as MultiStatement_Insert
,zeroifnull(sum(cast(trim(substr(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'InsSel=')+7,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Upd=')-
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'InsSel=')-7)) as INT))
  ) as MultiStatement_InsertSel
,zeroifnull(sum(cast(trim(substr(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Upd=')+4,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,' Sel=')-
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Upd=')-4)) as INT))
  ) as MultiStatement_Update
,zeroifnull(sum(cast(trim(substr(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,' Sel=')+5, 10)) as INT))
  ) as MultiStatement_Select

/* METRIC BREAK-OUTS:  */

/* Query Runtime by [query count | cpu | iogb] */
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  is NULL OR  dbql.TotalFirstRespTime <1     THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_runtime_0000_0001
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=1    AND  dbql.TotalFirstRespTime <5     THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_runtime_0001_0005
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=5    AND  dbql.TotalFirstRespTime <10    THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_runtime_0005_0010
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=10   AND  dbql.TotalFirstRespTime <30    THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_runtime_0010_0030
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=30   AND  dbql.TotalFirstRespTime <60    THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_runtime_0030_0060
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=60   AND  dbql.TotalFirstRespTime <300   THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_runtime_0060_0300
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=300  AND  dbql.TotalFirstRespTime <600   THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_runtime_0300_0600
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=600  AND  dbql.TotalFirstRespTime <1800  THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_runtime_0600_1800
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=1800 AND  dbql.TotalFirstRespTime <3600  THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_runtime_1800_3600
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >3600                                      THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_runtime_3600_plus

,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  is NULL OR  dbql.TotalFirstRespTime <1     THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_runtime_0000_0001
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=1    AND  dbql.TotalFirstRespTime <5     THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_runtime_0001_0005
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=5    AND  dbql.TotalFirstRespTime <10    THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_runtime_0005_0010
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=10   AND  dbql.TotalFirstRespTime <30    THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_runtime_0010_0030
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=30   AND  dbql.TotalFirstRespTime <60    THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_runtime_0030_0060
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=60   AND  dbql.TotalFirstRespTime <300   THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_runtime_0060_0300
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=300  AND  dbql.TotalFirstRespTime <600   THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_runtime_0300_0600
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=600  AND  dbql.TotalFirstRespTime <1800  THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_runtime_0600_1800
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=1800 AND  dbql.TotalFirstRespTime <3600  THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_runtime_1800_3600
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >3600                                      THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_runtime_3600_plus

,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  is NULL OR  dbql.TotalFirstRespTime <1     THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_runtime_0000_0001
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=1    AND  dbql.TotalFirstRespTime <5     THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_runtime_0001_0005
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=5    AND  dbql.TotalFirstRespTime <10    THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_runtime_0005_0010
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=10   AND  dbql.TotalFirstRespTime <30    THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_runtime_0010_0030
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=30   AND  dbql.TotalFirstRespTime <60    THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_runtime_0030_0060
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=60   AND  dbql.TotalFirstRespTime <300   THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_runtime_0060_0300
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=300  AND  dbql.TotalFirstRespTime <600   THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_runtime_0300_0600
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=600  AND  dbql.TotalFirstRespTime <1800  THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_runtime_0600_1800
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >=1800 AND  dbql.TotalFirstRespTime <3600  THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_runtime_1800_3600
,zeroifnull(SUM(CASE WHEN  dbql.TotalFirstRespTime  >3600                                      THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_runtime_3600_plus


/* delaytime by [query count | cpu | iogb] */
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  is NULL OR  dbql.delaytime <1     THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_delaytime_0000_0001
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=1    AND  dbql.delaytime <5     THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_delaytime_0001_0005
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=5    AND  dbql.delaytime <10    THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_delaytime_0005_0010
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=10   AND  dbql.delaytime <30    THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_delaytime_0010_0030
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=30   AND  dbql.delaytime <60    THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_delaytime_0030_0060
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=60   AND  dbql.delaytime <300   THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_delaytime_0060_0300
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=300  AND  dbql.delaytime <600   THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_delaytime_0300_0600
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=600  AND  dbql.delaytime <1800  THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_delaytime_0600_1800
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=1800 AND  dbql.delaytime <3600  THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_delaytime_1800_3600
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >3600                             THEN CAST(dbql.Statements as BIGINT) ELSE 0 END))   as qrycnt_in_delaytime_3600_plus

,zeroifnull(SUM(CASE WHEN  dbql.delaytime  is NULL OR  dbql.delaytime <1     THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_delaytime_0000_0001
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=1    AND  dbql.delaytime <5     THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_delaytime_0001_0005
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=5    AND  dbql.delaytime <10    THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_delaytime_0005_0010
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=10   AND  dbql.delaytime <30    THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_delaytime_0010_0030
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=30   AND  dbql.delaytime <60    THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_delaytime_0030_0060
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=60   AND  dbql.delaytime <300   THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_delaytime_0060_0300
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=300  AND  dbql.delaytime <600   THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_delaytime_0300_0600
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=600  AND  dbql.delaytime <1800  THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_delaytime_0600_1800
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=1800 AND  dbql.delaytime <3600  THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_delaytime_1800_3600
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >3600                             THEN CAST(dbql.AMPCPUtime + dbql.ParserCPUTime as decimal(18,4)) ELSE 0 END))   as cpusec_in_delaytime_3600_plus

,zeroifnull(SUM(CASE WHEN  dbql.delaytime  is NULL OR  dbql.delaytime <1     THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_delaytime_0000_0001
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=1    AND  dbql.delaytime <5     THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_delaytime_0001_0005
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=5    AND  dbql.delaytime <10    THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_delaytime_0005_0010
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=10   AND  dbql.delaytime <30    THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_delaytime_0010_0030
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=30   AND  dbql.delaytime <60    THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_delaytime_0030_0060
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=60   AND  dbql.delaytime <300   THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_delaytime_0060_0300
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=300  AND  dbql.delaytime <600   THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_delaytime_0300_0600
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=600  AND  dbql.delaytime <1800  THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_delaytime_0600_1800
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >=1800 AND  dbql.delaytime <3600  THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_delaytime_1800_3600
,zeroifnull(SUM(CASE WHEN  dbql.delaytime  >3600                             THEN CAST(ReqIOKB/1e6 as decimal(18,4)) ELSE 0 END))   as iogb_in_delaytime_3600_plus


From {dbqlogtbl} as dbql       /* pdcrinfo.dbqlogtbl_hst, typically */
/* TODO: union with DBQL_Summary table - Paul */

join dim_app as app
  on dbql.AppID = app.AppID

join dim_Statement stm
  on dbql.StatementType = stm.StatementType

join dim_user usr
  on dbql.UserName = usr.UserName

join (
      Select TheDate as LogDate_mi, Floor(TheTime/1e4) as LogHour_mi
      ,sum(cast(FullPotentialIOTA/1e6 as decimal(18,0))) as MaxIOTA_cntM
      from {resusagespma}  /* pdcrinfo.ResUsageSPMA_Hst */
      where TheDate between {startdate} and {enddate}
      Group by LogDate_mi, LogHour_mi
     ) maxiota
  on dbql.LogDate = maxiota.LogDate_mi
 and LogHour = maxiota.LogHour_mi

where dbql.LogDate between {startdate} and {enddate}

Group by
 dbql.LogDate
,LogHour
,maxiota.LogDate_mi
,maxiota.LogHour_mi
,SiteID
,app.App_Bucket
,app.Use_Bucket
,stm.Statement_Bucket
,usr.User_Bucket
,usr.Is_Discrete_Human
,usr.User_Department
,usr.User_SubDepartment
,usr.User_Region
,dbql.WDName
,Query_Type
,Total_AMPs
;

drop table dim_app;
drop table dim_statement;
drop table dim_user;
