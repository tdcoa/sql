/* Pulls all DBQL Data Required for CSM - CONSUMPTION ANALYTICS (COA)
   see comments about each SQL step inline below.

Parameters:
  - startdate:        "start date logic"
  - enddate:          "end date logic"
  - siteid:           "CIS site id for the system running this query set"
  - account:          "CIS account name"
  - default_database: "only needed  if you don't qualify tables below"
  - dbqlogtbl_hst:    "table name: [dbc||pdcrinfo||other].dbqlogtbl[_hst]"
  - resusagescpu_hst: "table name: [dbc||pdcrinfo||other].resusagescpu_hst"
*/


Database {default_database};

/*
DBQL - CORE PULL
-------------------
Pull from DBQL the core performance data set (subset of all DBQL).
All other SQL / Processes below use this volatile table instead of
perm DBQL table, in order to isolate change (dbc, pdcr, other, etc.)
to this single statement.  i.e., if specific column/logic changes are
needed, they can be done once here, and allowed to propogate.
*/

Create volatile Table dat_DBQL_Detail as
(
    Select
     LogDate
    ,extract(Hour from StartTime) as LogHour
    ,StartTime as LogTS
    /* LogDate is derived from StartTime.  who knew? */

    /* ---------- Workload Dimensions  */
    ,StatementType
    ,StatementGroup
    ,UserName
    ,DefaultDatabase
    ,AcctString
    ,trim(AppID) as AppID
    ,ClientID
    ,QueryBand
    ,WDName
    ,ProfileName
    ,NumOfActiveAMPs
    ,HashAmp()+1 as Total_AMPs

    /* ---------- Metrics: Query */
    ,1 as Request_Count
    ,Statements as Query_Count
    ,NumSteps * character_length(QueryText)/100 as Query_Complexity_Score
    ,NumResultRows as Returned_Row_Cnt
    ,case when ErrorCode=0 then 0 else 1 end as Query_Error_Cnt
    ,case when Abortflag='Y' then 1  else 0 end as Query_Abort_Cnt
    ,EstResultRows as Explain_Plan_Row_Cnt
    ,EstProcTime as Explain_Plan_Runtime_Sec


    /* ---------- Metrics: RunTimes */
    ,((FirstStepTime - StartTime) HOUR(3) TO SECOND(6)) AS Runtime_Parse
    ,((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) AS Runtime_AMP
    ,((FirstRespTime - StartTime) HOUR(3) TO SECOND(6)) as Runtime_Total
    ,case when LastRespTime is not null then ((LastRespTime - FirstRespTime) HOUR(3) TO SECOND(6)) end as TransferTime

    ,DelayTime as DelayTime_Sec
    ,ZEROIFNULL(CAST(EXTRACT(HOUR   FROM Runtime_Parse) * 3600
                   + EXTRACT(MINUTE FROM Runtime_Parse) * 60
                   + EXTRACT(SECOND FROM Runtime_Parse) as FLOAT))    as Runtime_Parse_Sec
    ,ZEROIFNULL(CAST(EXTRACT(HOUR   FROM Runtime_AMP) * 3600
                   + EXTRACT(MINUTE FROM Runtime_AMP) * 60
                   + EXTRACT(SECOND FROM Runtime_AMP) as FLOAT)) as Runtime_AMP_Sec
    ,TotalFirstRespTime  as Runtime_Total_Sec
    ,ZEROIFNULL(CAST(EXTRACT(HOUR   FROM TransferTime) * 3600
                   + EXTRACT(MINUTE FROM TransferTime) * 60
                   + EXTRACT(SECOND FROM TransferTime) as FLOAT)) AS TransferTime_Sec

    /* --- for later concurrency calcs: */
    ,StartTime as Request_Start_Time
    ,coalesce(LastRespTime, FirstRespTime) as Request_Complete_Time

    /* ---------- Metrics: CPU & IO */
    ,ParserCPUTime as CPU_Parse_Sec
    ,AMPCPUtime as CPU_AMP_Sec
    ,CPU_Parse_Sec + CPU_AMP_Sec as CPU_Total_DBS_Sec

    ,ReqPhysIO/1e6    as IOCntM_Physical
    ,case when IOCntM_Total>IOCntM_Physical then IOCntM_Total-IOCntM_Physical end as IOCntM_Cached
    ,TotalIOCount/1e6 as IOCntM_Total
    ,ReqPhysIOKB/1e6  as IOGB_Physical
    ,case when IOGB_Total>IOGB_Physical then IOGB_Total-IOGB_Physical end as IOGB_Cached
    ,ReqIOKB/1e6      as IOGB_Total
    ,cast(IOGB_Physical as decimal(18,6)) / nullifzero(IOGB_Total) as IOGB_Cache_Pct

    /* ---------- Metrics: Other */
    ,SpoolUsage/1e9 as Spool_GB
    ,(MaxStepMemory * nullifzero(NumOfActiveAMPs)) as Memory_MaxUsed_MB
    ,(AMPCPUTime / nullifzero(MaxAmpCPUTime*NumOfActiveAMPs))-1 as CPU_Skew_Pct
    ,(TotalIOCount / nullifzero(MaxAmpIO*NumOfActiveAMPs))-1 as IOCnt_Skew_Pct
    ,VHPhysIO / nullifzero(VHLogicalIO) as VeryHot_IOcnt_Cache_Pct
    ,VHPhysIOKB / nullifzero(VHLogicalIOKB) as VeryHot_IOGB_Cache_Pct
    ,case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end as tmpSG

    ,QueryID  /* -- For PI / Joining */

    From {dbqlogtbl_hst} as dbql

    Where LogDate between cast({startdate} as date) and cast({enddate} as date)

) with Data
/* match source table for optimal copy speed */
PRIMARY INDEX ( LogDate ,QueryID )
PARTITION BY RANGE_N(LogDate between cast({startdate} as date) and cast({enddate} as date)
  EACH INTERVAL '1' DAY )
on commit preserve rows;



/* create volatile table dat_cpu_seconds
   we do need ResUsage for CPU max seconds (kinda), and
   to find the percent of DBS / OS / IOWait / Idle.
   This is interesting enough to export & keep */
/*{{save:{siteid}.cpu_seconds.csv}}*/
/*{{load:adlste_coa.tmp_cpu_seconds}}*/
create volatile table dat_cpu_seconds
as (
    select TheDate as LogDate
    ,cast(cast((TheTime (format '999999')) as char(2)) as integer) as LogHour
    ,count(distinct CpuId) as vCores_Per_Node
    ,count(distinct NodeID) as Node_Cnt
    ,cast(sum((CPUIdle+CPUIoWait+CPUUServ+CPUUExec)/100) as decimal(18,2)) as CPU_MaxAvailable
    ,CPU_MaxAvailable / Node_Cnt / vCores_Per_Node / (60*60) as Reconcile_to_1sec
    ,sum(CPUIdle/100)  / CPU_MaxAvailable  as CPU_Idle_Pct
    ,sum(CPUIoWait/100)/ CPU_MaxAvailable  as CPU_IoWait_Pct
    ,sum(CPUUServ/100) / CPU_MaxAvailable  as CPU_OS_Pct
    ,sum(CPUUExec/100) / CPU_MaxAvailable  as CPU_DBS_Pct
    from {resusagescpu_hst}
    where TheDate between {startdate} and {enddate}
    group by LogDate, LogHour
) with data
no primary index
on commit preserve rows
;



/*{{temp:dim_app.coa.csv}}*/
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
  from (select distinct AppID from dat_DBQL_Detail) as o
  left join "dim_app.coa.csv" as p
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

drop table "dim_app.coa.csv";


/*{{temp:dim_statement.coa.csv}}*/
create volatile table dim_statement as
(
  select
   o.StatementType
  ,coalesce(p.Statement_Bucket,'Unknown') as Statement_Bucket
  ,coalesce(p.Priority,1e6) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.StatementType) as Pattern
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (select distinct StatementType from dat_DBQL_Detail) as o
  left join "dim_statement.coa.csv"  as p
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

drop table "dim_statement.coa.csv";


/*{{temp:dim_user.coa.csv}}*/
create volatile table dim_user as
(
  select
   o.UserName
  ,coalesce(p.User_Bucket,'Unknown') as User_Bucket
  ,coalesce(p.Is_Discrete_Human,'Unk') as Is_Discrete_Human
  ,coalesce(p.User_Department, 'Unknown') as User_Department
  ,coalesce(p.User_SubDepartment, 'Unknown') as User_SubDepartment
  ,coalesce(p.User_Region, 'Unknown') as User_Region
  ,coalesce(p.Priority,1e6) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.UserName) as Pattern
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (select distinct UserName from dat_DBQL_Detail) as o
  left join "dim_user.coa.csv" as p
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

drop table "dim_user.coa.csv";




/*
------- DAT_DBQL  (Final Output)
---------------------------------------------------------------
Aggregates earlier DBQL snapshot into a per-day, per-hour, per-DIM bucket
as defined above.  The intention is this to be a much smaller table than the
detail, however, this assumption largely relies on how well the bucketing
logic is defined / setup above.  If the result set is too large, try
revisiting the Bucket definitions above, and make groups smaller / less varied.

Also - many compound metrics have been stripped, to minimize transfer file
size.  As these fields are easily calculated, they will be re-constituted
in Transcend.
*/

/*{{save:{siteid}.FINAL_dat_DBQL.csv}}*/
/*{{load:adlste_coa.tmp_dat_DBQL}}*/
select
 dbql.LogDate
,dbql.LogHour
,cast(cast(dbql.LogDate as char(10)) ||' '||
 cast(cast(dbql.LogHour as INT format '99') as char(2))||':00:00.000000'  as timestamp(6)) as LogTS
,'{account}' as AccountName
,'{siteid}'  as SiteID
,avg(cpumax.Node_Cnt) as Node_Cnt
,avg(cpumax.vCores_per_Node) as vCores_per_Node

/*--------- Workload Buckets: */
,app.App_Bucket
,app.Use_Bucket
,stm.Statement_Bucket
,usr.User_Bucket
,usr.Is_Discrete_Human
,usr.User_Department
,usr.User_SubDepartment
,usr.User_Region
,dbql.WDName as Workload_Name

,case when  dbql.StatementType = 'Select'
        and dbql.AppID not in ('TPTEXP', 'FASTEXP')
        and dbql.Runtime_AMP_Sec < 1
        and dbql.NumOfActiveAMPs < dbql.Total_AMPs
      then 'Tactical'
      else 'Non-Tactical'
      /* TODO: flesh out this logic to further refine Query_Types */
 end as Query_Type

/* -------- Query Metrics */
,sum(Query_Count) as Query_Cnt
,sum(Request_Count) as Request_Cnt
,avg(Query_Complexity_Score) as Query_Complexity_Score_Avg
,cast(sum(Returned_Row_Cnt) as decimal(18,0)) as Returned_Row_Cnt
,sum(Query_Error_Cnt) as Query_Error_Cnt
,sum(Query_Abort_Cnt) as Query_Abort_Cnt
,sum(Explain_Plan_Row_Cnt) as Explain_Plan_Row_Cnt
,sum(Explain_Plan_Runtime_Sec) as Explain_Plan_Runtime_Sec


/* --------- Metrics: RunTimes */
,round( sum(DelayTime_Sec)     ,2) as DelayTime_Sec
,round( sum(Runtime_Parse_Sec) ,2) as Runtime_Parse_Sec
,round( sum(Runtime_AMP_Sec)   ,2) as Runtime_AMP_Sec
,round( sum(TransferTime_Sec)  ,2) as TransferTime_Sec
/*-- Runtime_Parse_Sec + Runtime_AMP_Sec = Runtime_Execution_Sec
-- DelayTime_Sec + Runtime_Execution_Sec + TransferTime_Sec as Runtime_UserExperience_Sec */



/*---------- Metrics: CPU & IO */
,cast( sum(dbql.CPU_Parse_Sec) as decimal(18,2)) as CPU_Parse
,cast( sum(dbql.CPU_AMP_Sec) as decimal(18,2)) as CPU_AMP
/* -- ,CPU_Parse + CPU_AMP as CPU_Total_DBS */
,cast( sum(cpumax.CPU_MaxAvailable) as decimal(18,2)) as CPU_MaxAvailable_perHour
/* -- ,vCores_Per_Node * Node_Cnt * Runtime_Total_Sec    as CPU_MaxAvailable_perRuntime */
,cast(avg(cpumax.CPU_DBS_Pct) as decimal(18,6))    as CPU_DBS_Pct
,cast(avg(cpumax.CPU_OS_Pct)  as decimal(18,6))    as CPU_OS_Pct
,cast(avg(cpumax.CPU_IoWait_Pct) as decimal(18,6)) as CPU_IoWait_Pct
,cast(avg(cpumax.CPU_Idle_Pct) as decimal(18,6))   as CPU_Idle_Pct



,sum(IOCntM_Physical) as IOCntM_Physical
,sum(IOCntM_Cached)   as IOCntM_Cached
,sum(IOCntM_Total)    as IOCntM_Total
,sum(IOGB_Physical)   as IOGB_Physical
,sum(IOGB_Cached)     as IOGB_Cached
,sum(IOGB_Total)      as IOGB_Total
,sum(IOGB_Cache_Pct)  as IOGB_Cache_Pct

,NULL as IOGB_Total_Max   /* TODO */


/* ---------- Metrics: Other */
,sum(Spool_GB) as Spool_GB
,avg(Spool_GB) as Spool_GB_Avg
,sum(Memory_MaxUsed_MB) as Memory_Max_Used_MB
,avg(Memory_MaxUsed_MB) as Memory_Max_Used_MB_Avg
,avg(CPU_Skew_Pct) as CPUSec_Skew_AvgPCt
,avg(IOCnt_Skew_Pct)  as IOCnt_Skew_AvgPct
,avg(VeryHot_IOcnt_Cache_Pct) as VeryHot_IOcnt_Cache_AvgPct
,avg(VeryHot_IOGB_Cache_Pct) as VeryHot_IOGB_Cache_AvgPct

/* ---------- Multi-Statement Break-Out, if interested: */
,count(tmpSG) as MultiStatement_Count
,sum(cast(trim(substr(tmpSG, index(tmpSG,'Del=')+4, index(tmpSG,'Ins=')-index(tmpSG,'Del=')-4)) as INT))       as MultiStatement_Delete
,sum(cast(trim(substr(tmpSG, index(tmpSG,'Ins=')+4, index(tmpSG,'InsSel=')-index(tmpSG,'Ins=')-4)) as INT))    as MultiStatement_Insert
,sum(cast(trim(substr(tmpSG, index(tmpSG,'InsSel=')+7, index(tmpSG,'Upd=')-index(tmpSG,'InsSel=')-7)) as INT)) as MultiStatement_InsertSel
,sum(cast(trim(substr(tmpSG, index(tmpSG,'Upd=')+4, index(tmpSG,' Sel=')-index(tmpSG,'Upd=')-4)) as INT))      as MultiStatement_Update
,sum(cast(trim(substr(tmpSG, index(tmpSG,' Sel=')+5, 10)) as INT)) as MultiStatement_Select

From dat_DBQL_detail as dbql

join dat_cpu_seconds as cpumax
  on dbql.LogDate = cpumax.LogDate
 and dbql.LogHour = cpumax.LogHour

join dim_app as app
  on dbql.AppID = app.AppID

join dim_Statement stm
  on dbql.StatementType = stm.StatementType

join dim_user usr
  on dbql.UserName = usr.UserName

Group by
     dbql.LogDate
    ,dbql.LogHour
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
;

drop table dat_cpu_seconds;
drop table dim_app;
drop table dim_statement;
drop table dim_user;



/*
------- DAT_USER_RANKS
------------------------------------
Generate ranks for most active users, both with ALL accounts,
then again for all UserNames known to be discrete_humans.
*/
Create Volatile Table dat_user_ranks as
(
 select a.*, rank() over(Order by Overall_Score, Query_Count, CPU_Total_DBS_Sec) as Overall_Rank
 from (
    Select UserName
    ,cast('All' as varchar(25)) as Rank_Bucket
    ,sum(cast(Query_Count as decimal(18,0))) as Query_Count
    ,rank() over(Order by sum(cast(Query_Count as decimal(18,0))) desc) as Query_Count_Rank
    ,avg(cast(Query_Complexity_Score as decimal(18,0))) as Query_Complexity_Score
    ,rank() over(Order by avg(cast(Query_Complexity_Score as decimal(18,0))) desc) as Query_Complexity_Rank
    ,sum(cast(CPU_Total_DBS_Sec as decimal(18,2))) as CPU_Total_DBS_Sec
    ,rank() over(Order by sum(cast(CPU_Total_DBS_Sec as decimal(18,2))) desc) as CPU_Total_DBS_Rank
    ,sum(cast(IOGB_Total as decimal(18,2))) as IOGB_Total
    ,rank() over(Order by sum(cast(IOGB_Total as decimal(18,2))) desc) as IOGB_Total_Rank
    ,sum(cast(Runtime_Total_Sec as decimal(18,2))) as Runtime_Total_Sec
    ,rank() over(Order by sum(cast(Runtime_Total_Sec as decimal(18,2))) desc) as Runtime_Total_Rank
    ,(Query_Count_Rank + Query_Complexity_Rank + CPU_Total_DBS_Rank +
      IOGB_Total_Rank + Runtime_Total_Rank) as Overall_Score
    from dat_DBQL_Detail as dbql
    group by UserName
 ) as a
) with data
No Primary Index
on commit preserve rows
;

Insert into coat_dat_user_ranks
 select a.*, rank() over(Order by Overall_Score, Query_Count, CPU_Total_DBS_Sec) as Overall_Rank
 from (
    Select UserName
    ,cast('Users' as varchar(25)) as Rank_Bucket
    ,sum(cast(Query_Count as decimal(18,0))) as Query_Count
    ,rank() over(Order by sum(cast(Query_Count as decimal(18,0))) desc) as Query_Count_Rank
    ,avg(cast(Query_Complexity_Score as decimal(18,0))) as Query_Complexity_Score
    ,rank() over(Order by avg(cast(Query_Complexity_Score as decimal(18,0))) desc) as Query_Complexity_Rank
    ,sum(cast(CPU_Total_DBS_Sec as decimal(18,2))) as CPU_Total_DBS_Sec
    ,rank() over(Order by sum(cast(CPU_Total_DBS_Sec as decimal(18,2))) desc) as CPU_Total_DBS_Rank
    ,sum(cast(IOGB_Total as decimal(18,2))) as IOGB_Total
    ,rank() over(Order by sum(cast(IOGB_Total as decimal(18,2))) desc) as IOGB_Total_Rank
    ,sum(cast(Runtime_Total_Sec as decimal(18,2))) as Runtime_Total_Sec
    ,rank() over(Order by sum(cast(Runtime_Total_Sec as decimal(18,2))) desc) as Runtime_Total_Rank
    ,(Query_Count_Rank + Query_Complexity_Rank + CPU_Total_DBS_Rank +
      IOGB_Total_Rank + Runtime_Total_Rank) as Overall_Score
    from dat_DBQL_Detail as dbql
    where UserName in(select UserName from dim_user where Is_Discrete_Human = 'yes')
    group by UserName
 ) as a
;

/*{{save:{siteid}.dat_user_ranks.csv}}*/
/*{{load:adlste_coa.tmp_user_ranks}}*/
select * from coat_dat_user_ranks order by Rank_Bucket, Overall_Rank
;


drop table dat_DBQL_Detail;
drop table coat_dat_user_ranks;
