/* Pulls all DBQL Data Required for CSM - CONSUMPTION ANALYTICS (COA)
   see comments about each SQL step inline below.

Parameters:
  - startdate:        "start date logic"
  - enddate:          "end date logic"
  - siteid:           "CIS site id for the system running this query set"
  - account:          "CIS account name"
  - default_database: "only needed  if you don't qualify tables below"
  - dbqlogtbl_hst:    "table name: [dbc||pdcrinfo||other].dbqlogtbl[_hst]"
*/


Database {default_database};

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
  from (select distinct AppID from {dbqlogtbl_hst}
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
  from (select distinct StatementType from {dbqlogtbl_hst}
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
  from (select distinct UserName from {dbqlogtbl_hst}
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

drop table "dim_user.csv";



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

/*{{save:0001.DBQL_Summary.OUTPUT-{siteid}.csv}}*/
/*{{load:adlste_coa.stg_dat_DBQL}}*/
/*{{call:adlste_coa.sp_dat_dbql()}}*/
select
 dbql.LogDate
,dbql.LogHour
,cast(cast(dbql.LogDate as char(10)) ||' '||
 cast(cast(dbql.LogHour as INT format '99') as char(2))||':00:00.000000'  as timestamp(6)) as LogTS
,'{siteid}'  as SiteID

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
 when  dbql.StatementType = 'Select'
  and dbql.AppID not in ('TPTEXP', 'FASTEXP')
  and dbql.Runtime_AMP_Sec < 1
  and dbql.NumOfActiveAMPs < Total_AMPs
 then 'Tactical'
 else 'Non-Tactical'
      /* TODO: Query_Type */
 end as Query_Type


/* -------- Query Metrics */
 ,HashAmp()+1 as Total_AMPs
,sum(dbql.Statements) as Query_Cnt
,count(1) as Request_Cnt
,avg(NumSteps * character_length(QueryText)/100) as Query_Complexity_Score_Avg
,sum(cast(NumResultRows as decimal(18,0))) as Returned_Row_Cnt
,sum(case when ErrorCode=0 then 0 else 1 end) as Query_Error_Cnt
,sum(case when Abortflag='Y' then 1  else 0 end) as Query_Abort_Cnt
,sum(cast(EstResultRows/1e6 as decimal(18,0))) as Explain_Row_Cnt
,sum(EstProcTime) as Explain_Runtime_Sec


/* --------- Metrics: RunTimes */
,sum(round(sum(DelayTime),2)) as DelayTime_Sec

/* TODO: RunTimes */
,100 as Runtime_Parse_Sec
,1e6 as Runtime_AMP_Sec
,1e6 as TransferTime_Sec

/*
,((FirstStepTime - StartTime) HOUR(3) TO SECOND(6)) AS Runtime_Parse
,((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) AS Runtime_AMP
,((FirstRespTime - StartTime) HOUR(3) TO SECOND(6)) as Runtime_Total
,case when LastRespTime is not null then ((LastRespTime - FirstRespTime) HOUR(3) TO SECOND(6)) end as TransferTime

,DelayTime as DelayTime_Sec
,ZEROIFNULL(CAST(EXTRACT(HOUR   FROM Runtime_Parse) * 3600
               + EXTRACT(MINUTE FROM Runtime_Parse) * 60
               + EXTRACT(SECOND FROM Runtime_Parse) as FLOAT)) as Runtime_Parse_Sec
,ZEROIFNULL(CAST(EXTRACT(HOUR   FROM Runtime_AMP) * 3600
               + EXTRACT(MINUTE FROM Runtime_AMP) * 60
               + EXTRACT(SECOND FROM Runtime_AMP) as FLOAT)) as Runtime_AMP_Sec
,TotalFirstRespTime  as Runtime_Total_Sec
,ZEROIFNULL(CAST(EXTRACT(HOUR   FROM TransferTime) * 3600
               + EXTRACT(MINUTE FROM TransferTime) * 60
               + EXTRACT(SECOND FROM TransferTime) as FLOAT)) AS TransferTime_Sec
*/


/*-- Runtime_Parse_Sec + Runtime_AMP_Sec = Runtime_Execution_Sec
-- DelayTime_Sec + Runtime_Execution_Sec + TransferTime_Sec as Runtime_UserExperience_Sec */



/*---------- Metrics: CPU & IO */
,cast( sum(dbql.ParserCPUTime) as decimal(18,2)) as CPU_Parse_Sec
,cast( sum(dbql.AMPCPUtime) as decimal(18,2)) as CPU_AMP_Sec

,sum(ReqPhysIO/1e6)    as IOCntM_Physical
,sum(TotalIOCount/1e6) as IOCntM_Total
,sum(ReqPhysIOKB/1e6)  as IOGB_Physical
,sum(ReqIOKB/1e6)      as IOGB_Total

/* TODO: IOTAs  */
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
/*
,case when dbql.StatementGroup like 'DML Del=%' then dbql.StatementGroup end as tmpSG
,sum(cast(trim(substr(tmpSG, index(tmpSG,'Del=')+4, index(tmpSG,'Ins=')-index(tmpSG,'Del=')-4)) as INT))       as MultiStatement_Delete
,sum(cast(trim(substr(tmpSG, index(tmpSG,'Ins=')+4, index(tmpSG,'InsSel=')-index(tmpSG,'Ins=')-4)) as INT))    as MultiStatement_Insert
,sum(cast(trim(substr(tmpSG, index(tmpSG,'InsSel=')+7, index(tmpSG,'Upd=')-index(tmpSG,'InsSel=')-7)) as INT)) as MultiStatement_InsertSel
,sum(cast(trim(substr(tmpSG, index(tmpSG,'Upd=')+4, index(tmpSG,' Sel=')-index(tmpSG,'Upd=')-4)) as INT))      as MultiStatement_Update
,sum(cast(trim(substr(tmpSG, index(tmpSG,' Sel=')+5, 10)) as INT)) as MultiStatement_Select
*/

/* TODO: Multi-Statement */
,100 as MultiStatement_Delete
,100 as MultiStatement_Insert
,100 as MultiStatement_InsertSel
,100 as MultiStatement_Update

From {dbqlogtbl_hst} as dbql

join dim_app as app
  on dbql.AppID = app.AppID

join dim_Statement stm
  on dbql.StatementType = stm.StatementType

join dim_user usr
  on dbql.UserName = usr.UserName

where dbql.logdate between {startdate} and {enddate}

Group by
 dbql.LogDate
,dbql.LogHour
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
