/* Pulls all DBQL Data Required for CSM - CONSUMPTION ANALYTICS (COA)
   see comments about each SQL step inline below.

Parameters:
  - startdate:    {startdate}
  - enddate:      {enddate}
  - siteid:       {siteid}
  - dbqlogtbl:    {dbqlogtbl}
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


/*{{temp:dim_user.csv}}*/
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


/*{{file:custom_user_dim.sql}}*/
;

/*
------- DAT_DBQL  (Final Output)
---------------------------------------------------------------
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
/*{{load:adlste_coa.stg_dat_DBQL}}*/
/*{{call:adlste_coa.sp_dat_dbql('{fileset_version}')}}*/
select top 20
 '{siteid}'  as SiteID
,dbql.LogDate
,cast(extract(HOUR from StartTime) as INT format '99') as LogHour
,cast(cast(dbql.LogDate as char(10)) ||' '||
 cast(LogHour as char(2))||':00:00.000000'  as timestamp(6)) as LogTS

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
  and app.App_Bucket not in ('TPTEXP', 'FASTEXP')
  and (ZEROIFNULL(CAST(
     (EXTRACT(HOUR   FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) * 3600)
    +(EXTRACT(MINUTE FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) *   60)
    +(EXTRACT(SECOND FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) *    1)
     as FLOAT))) < 1  /* Runtime_AMP_Sec */
  and dbql.NumOfActiveAMPs < Total_AMPs
 then 'Tactical'
 else 'Non-Tactical'
/* TODO: Query_Type -- design other query types */
 end as Query_Type

/* -------- Query Metrics */
 ,HashAmp()+1 as Total_AMPs
,sum(dbql.Statements) as Query_Cnt
,count(1) as Request_Cnt
,avg(dbql.NumSteps * character_length(dbql.QueryText)/100) as Query_Complexity_Score_Avg
,sum(cast(dbql.NumResultRows as decimal(18,0))) as Returned_Row_Cnt
,sum(case when dbql.ErrorCode=0 then 0 else 1 end) as Query_Error_Cnt
,sum(case when dbql.Abortflag='Y' then 1  else 0 end) as Query_Abort_Cnt
,sum(cast(dbql.EstResultRows/1e6 as decimal(18,0))) as Explain_Row_Cnt
,sum(dbql.EstProcTime) as Explain_Runtime_Sec


/* --------- Metrics: RunTimes */
,sum(round(dbql.DelayTime,2)) as DelayTime_Sec
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
/*---------- Metrics: CPU & IO */
,cast( sum(dbql.ParserCPUTime) as decimal(18,2)) as CPU_Parse_Sec
,cast( sum(dbql.AMPCPUtime) as decimal(18,2)) as CPU_AMP_Sec
/* TODO: check if failed queries log CPU consumption */

,sum(ReqPhysIO/1e6)    as IOCntM_Physical
,sum(TotalIOCount/1e6) as IOCntM_Total
,sum(ReqPhysIOKB/1e6)  as IOGB_Physical
,sum(ReqIOKB/1e6)      as IOGB_Total

,1e9 as IOTA_Total
,1e9 as IOGB_Total_Max


/* ---------- Metrics: Other */
,avg(NumOfActiveAMPs) as NumOfActiveAMPs_Avg
,sum(SpoolUsage/1e9) as Spool_GB
,avg(SpoolUsage/1e9) as Spool_GB_Avg
,sum(MaxStepMemory * nullifzero(NumOfActiveAMPs)) as Memory_Max_Used_MB
,avg(MaxStepMemory * nullifzero(NumOfActiveAMPs)) as Memory_Max_Used_MB_Avg
,avg((AMPCPUTime / nullifzero(MaxAmpCPUTime*NumOfActiveAMPs))-1) as CPUSec_Skew_AvgPCt
,avg((TotalIOCount / nullifzero(MaxAmpIO*NumOfActiveAMPs))-1)  as IOCnt_Skew_AvgPct
,avg(VHPhysIO / nullifzero(VHLogicalIO)) as VeryHot_IOcnt_Cache_AvgPct
,avg(VHPhysIOKB / nullifzero(VHLogicalIOKB)) as VeryHot_IOGB_Cache_AvgPct

/* ---------- Multi-Statement Break-Out, if interested: */
,count(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end) as MultiStatement_Count
,sum(cast(trim(
    substr(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Del=')+4,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Ins=')-
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Del=')-4)) as INT))
    as MultiStatement_Delete
,sum(cast(trim(substr(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Ins=')+4,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'InsSel=')-
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Ins=')-4)) as INT))
    as MultiStatement_Insert
,sum(cast(trim(substr(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'InsSel=')+7,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Upd=')-
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'InsSel=')-7)) as INT))
    as MultiStatement_InsertSel
,sum(cast(trim(substr(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Upd=')+4,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,' Sel=')-
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,'Upd=')-4)) as INT))
    as MultiStatement_Update
,sum(cast(trim(substr(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,
    index(case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end,' Sel=')+5, 10)) as INT))
    as MultiStatement_Select


From {dbqlogtbl} as dbql
/* TODO: union with DBQL_Summary table -- Paul */

join dim_app as app
  on dbql.AppID = app.AppID

join dim_Statement stm
  on dbql.StatementType = stm.StatementType

join dim_user usr
  on dbql.UserName = usr.UserName

where dbql.logdate between {startdate} and {enddate}

Group by
 dbql.LogDate
,LogHour
,LogTS
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
