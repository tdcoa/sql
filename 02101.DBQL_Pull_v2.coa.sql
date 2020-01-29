/* Pulls all DBQL Data Required for CSM - CONSUMPTION ANALYTICS (COA)
   see comments about each SQL step inline below.

   DEPENDENCIES:
   - 00900.coat_dim_app.coa.csv   must exist
*/



Database {defaultdatabase};

/*
DBQL - CORE PULL
-------------------
Pull from DBQL the core performance data set (subset of all DBQL).
All other SQL / Processes below use this volatile table instead of
perm DBQL table, in order to isolate change.

i.e., if PDCR does not exist, replace with DBC here. If customer has
other non-standard approach, you can rewrite/replace this singular
"create volatile table" statement, and assuming all data types and
column names remain the same, all other SQL below will work.
*/

Create volatile Table coat_dat_DBQL_Detail as
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

    From {table_dbqlogtbl} as dbql

    Where LogDate between {startdate} and {enddate}

) with Data
Primary Index ( LogDate ,QueryID )  /* -- keep same PI as DBQL, for fastest table-copy & distribution */
partition by range_n (LogDate between {startdate} and {enddate} each interval '1' day)
on commit preserve rows
;

collect stats on coat_dat_dbql_detail column ( LogDate ,QueryID );
collect stats on coat_dat_dbql_detail column ( LogDate );




/*{{temp:00901.coat_dim_statement.coa.csv}}*/

/*{{temp:00900.coat_dim_app.coa.csv}}*/

/* #2) LEFT OUTER JOIN with DAT table to get all possible DIM values */
insert into "00900.coat_dim_app.coa.csv"
select artPI, SiteID, AppID, App_Bucket, Use_Bucket
,(row_number() over(order by AppID)*10)+100 as Priority
,Record_Status, Process_ID, Process_TS
from
    (
    select
     2 as ArtPI
    ,coalesce(app.SiteID, '{siteid}') as SiteID
    ,dbql.AppID
    ,coalesce(app.App_Bucket, 'Unknown') as App_Bucket
    ,coalesce(app.Use_Bucket, 'Unknown') as Use_Bucket
    ,coalesce(app.Priority, 32676) as Priority
    ,'Load with Real AppIDs' as Record_Status
    ,1 as Process_ID
    ,current_timestamp(0) as Process_TS
    from coat_dat_DBQL_detail as dbql
    left outer join "00900.coat_dim_app.coa.csv" as app
      on  app.ArtPI = 2
     and dbql.AppID like trim(app.AppID)
    Group by 1,2,3,4,5,6
) as a
qualify a.Priority = min(a.Priority) over (partition by a.AppID)
;

/*  #4) with all inserts done, delete all non-priority DIM records */
Delete from "00900.coat_dim_app.coa.csv" where Process_ID = 0 ;

/*  Export just to have a copy... */
Select * from "00900.coat_dim_app.coa.csv" where ArtPI=2 order by AppID;





/*
------- DIM_USER
------------------------------------
This Dimensional table provides a starting definition point
for bucketing that will happen in the final DBQL step.  That
bucketing is equal-join, so there are several steps required
between the fuzzy-match logic below (Process_ID=0) and the
final join list.

The USER table has several steps to its logic, as users will often
reside in more than one bucket.  Additionally, bucketing UserNames
is a more complex operation, often requiring special logic to handle
testing string formats against expected patterns.  For example,
Teradata's QuickLook ID is the employee's initials, plus a 6 digit
numner.   Finding a UserName that matches that pattern, regardless
of the letters or numners, makes it a QLID.  This special logic must
be coded specifically for each use-case.  One example is provided below,
in the {{subtraction logic}}.   This logic removes a set of characters
(numbers and underscore) then compaares the length of remaining characters.
If it matches, then the UserName qualifies for the record.  Depending
on the priority assigned, that record may or may not be the final value.

Also, the Business Usage Decoded values have been rolled into this table,
as granularity is identical.  This also allows you to build BUD mappings
using any of the below logic.

Broadly speaking, the process logic of the DIM tables is:
#1) define initial fuzzy LIKE matching logic ('%some%value%')
    - Process_ID=0, for easy identification
    - assign priority, highest wins if multiple qualififications exist
    - TBD: starting "Default" records will download from GitHub

#2) LEFT OUTER JOIN with DAT table to get all possible DIM values
    - DAT table is DBQL volatile snapshot, so no change during process
    - use fuzzy LIKE logic to match many records
    - insert specific values back into the DIM table
      - without wildcards, allowing equal-join
      - with higher priority (lower value) than fuzzy match logic
      - Process_ID = 1 (or higher, if multiple steps)
    - if multiple passes / logic sets, repeat above as needed

#3) manual verification / changes made
    - DIM now contains one-or-more record per value in DAT table
    - probbaly multiple records for every DIM value, but
    - with unique priority IDs, so highest can be identified

#4) with all inserts done, delete all non-priority DIM records
    - leaves DIM table with exactly 1 record per value in DAT table
    - now ready for equal-join

This logic seeks to do a few things:
a) uses priority fuzzy-match logic to minimize amount of manual
   maintenance / setup by the SE/CSM
b) shift logic complexity to the smaller DIM table, to keep the
   larger DAT table joins as simple and resource efficient as
   possible, thus being good stuarts of our customer's resources
c) provide the extensibility and flexibility needed to span across
   many accounts and environments, by abstracting the DIM process
   to one single table.  i.e., should this logic not suit, simply
   insert whatever other logic is required into the same structure
   and all down-stream logic should still work
*/


/*{{temp:00902.coat_dim_user.coa.csv}}*/


/* #2a) LEFT OUTER JOIN with DAT table to get all possible DIM values */
insert into "00902.coat_dim_user.coa.csv"
Select
 max(usr.artPI)  over() as artPI
,coalesce(usr.SiteID, max(usr.SiteID) over()) as SiteID
,dbql.UserName
,coalesce(usr.User_Bucket,      'Unknown') as User_Bucket
,coalesce(usr.Is_Discrete_Human, 'unk')    as Is_Discrete_Human
,usr.User_Department
,usr.User_SubDepartment
,usr.User_Region
,coalesce(usr.Priority-1, 32767) as Priority2
,'Normal Pattern Logic' as Record_Status
,1 as Process_ID
,Current_Timestamp(0) as Process_TS
from (Select distinct UserName from coat_dat_DBQL_Detail ) as dbql
left outer join "00902.coat_dim_user.coa.csv" usr
  on dbql.UserName like usr.UserName_Pattern
where usr.artpi=4
qualify Priority2 = min(Priority2) over(partition by dbql.UserName)
;

/* #2b) {{special logic}}, specifically the Subtraction Logic. */
Insert into "00902.coat_dim_user.coa.csv"
Select
 sub.artPI
,sub.SiteID
,dbql.UserName as UserName_Pattern
,sub.User_Bucket
,sub.Is_Discrete_Human
,sub.User_Department
,sub.User_SubDepartment
,sub.USer_Region
,sub.Priority + Row_Number() over(order by dbql.UserName) as Priority
,'Subtraction Logic' as Record_Status
,2 as Process_ID
,Current_Timestamp(0) as Process_TS
from (Select UserName from coat_dat_DBQL_Detail Group by UserName) as dbql
cross join (
    /* SUBTRACTION LOGIC:  if you remove certain characters (like all numbers),
    --   you can infer logic based on how many characters remain.
    --   i.e., Quicklook IDs: subtract 0123456789, then len should == 2 (remaining alpha)
    --   Ideally, change the values or expected length in the above insert, not here */
    Select
     ltrim(rtrim(UserName_Pattern, '}'),'{') as up
    ,ltrim(rtrim(trim(substr(up,(index(up,'=')+1),index(up,',')-(index(up,'=')+1))),'"'),'"') as subtract
    ,substr(substr(up, index(up,',')+1),index(substr(up, index(up,',')+1) ,'=')+1) as expected_length
    ,u.*
    from "00902.coat_dim_user.coa.csv" as u
    where artpi =4
      and substr(UserName_Pattern,1,10) = '{{subtract'
) as sub
where character_length(ltrim(rtrim(dbql.UserName, sub.subtract), sub.subtract))=sub.expected_length
;

collect stats on "00902.coat_dim_user.coa.csv" column (UserName_Pattern);
collect stats on "00902.coat_dim_user.coa.csv" column (artpi);



/*
------- DAT_USER_RANKS
------------------------------------
This pulls from the above DBQL snapshot and generates rankings for all users,
across all time.   It allows you to quickly see UserName of the most active
users, measured against a numnber of metrics (query count, complexity, CPU,
IO, and RunTime of all queries), as well as an overall aggregate score and
rank.  This is generated, exported, then optionally used in the DIM_USER below.

This is tucked inside the DIM_USER logic, as the Rank_Bucket of 'Users'
is added, representing only account UserNames that are "discrete_humans"
i.e., not applications or ETL.  The top users can then be added as one more
additional dimention breakout, for analysis purposes.
*/
Create Volatile Table coat_dat_user_ranks as
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
    from coat_dat_DBQL_Detail as dbql
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
    from coat_dat_DBQL_Detail as dbql
    where UserName in(select UserName_Pattern from "00902.coat_dim_user.coa.csv"
                      where artpi=4 and Is_Discrete_Human = 'yes')
    group by UserName
 ) as a
;

/*{{save:{siteid}.coat_dat_user_ranks.csv.sql}}*/
select * from coat_dat_user_ranks order by Rank_Bucket, Overall_Rank
;

Insert into "00902.coat_dim_user.coa.csv"
Select
 4 as artPI
,'Default' as SiteId
,UserName as UserName_Pattern
,'Top Consumer (' || rnk.Rank_Bucket || '): #' || cast(Overall_Rank as varchar(5))
    || ' (' || UserName ||
    case when Query_Count_Rank = 1      then ' - #1 Query Count'      else '' end ||
    case when Query_Complexity_Rank = 1 then ' - #1 Complexity' else '' end ||
    case when CPU_Total_DBS_Rank = 1        then ' - #1 CPU'  else '' end ||
    case when IOGB_Total_Rank = 1       then ' - #1 IO'   else '' end ||
    case when RunTime_Total_Rank = 1    then ' - #1 Runtime'    else '' end ||
    ')' as User_Bucket
,bud.Is_Discrete_Human
,bud.User_Department
,bud.User_SubDepartment
,bud.User_Region
,case when rnk.Rank_Bucket = 'All' then 1 else 2 end as Priority
,'Top User: ' || rnk.Rank_Bucket as Record_Status
,3 as Process_ID
,current_timestamp(0) as Process_ts
from  coat_dat_user_ranks rnk
left outer join
    (Select  /* highest priority BUD columns  */
     UserName_Pattern
    ,Priority
    ,Is_Discrete_Human
    ,User_Department
    ,User_SubDepartment
    ,User_Region
    from "00902.coat_dim_user.coa.csv" as u
    where artpi=4
      and (User_Department is not null or User_SubDepartment is not null or User_Region is not null)
    qualify Priority = min(u.priority) over(partition by UserName_Pattern)
    ) as bud
  on rnk.UserName = bud.UserName_Pattern
where Overall_Rank <=5
   or Query_Count_Rank = 1
   or Query_Complexity_Rank = 1
   or CPU_Total_DBS_Rank = 1
   or IOGB_Total_Rank = 1
   or RunTime_Total_Rank = 1
;

collect stats on "00902.coat_dim_user.coa.csv";

/* #4) with all inserts done, delete all non-priority DIM records
  -- Get rid of any wild-carded "initial load" records */
Delete from "00902.coat_dim_user.coa.csv" where artPI=4 and Process_ID = 0;

/* Identify all non-highest priority records, and mark for deletion
-- note, DELETE statements cannot contain QUALIFY, so this has to be
-- done with another volatile table. */
create volatile table coat_tmp_deleteusers as
(
    select artPI, UserName_Pattern, Priority
    from (select * from "00902.coat_dim_user.coa.csv" as u where artpi=4
          qualify Priority <> min(priority)over(partition by UserName_Pattern)) as a
    group by 1,2,3

) with data
primary index (artPI)
on commit preserve rows

/*  delete: */
Delete from "00902.coat_dim_user.coa.csv"
where artpi=4
  and (UserName_Pattern, Priority) in
  (select UserName_Pattern, Priority from coat_tmp_deleteusers where artpi=4)

drop table coat_tmp_deleteusers;

/*-- if any duplicates exist at this point, it's because the records have
-- the same UserName_Pattern AND Priority (naughty naughty,  shouldn't happen)
-- The correct fix here is to adjust your initial manual loads so they don't
-- overlap, but if you're lazy, below will arbitrarily pick a winner.
-- either way, it will not hurt to run. */
create volatile table coat_tmp_deleteusers as
(
 select * from "00902.coat_dim_user.coa.csv" as u  where artpi=4
 qualify row_number() over(partition by UserName_Pattern order by Priority)<>1
) with data
primary index (artPI)
on commit preserve rows;

Delete from "00902.coat_dim_user.coa.csv"
where
( artPI, SiteID, UserName_Pattern, User_Bucket, Is_Discrete_Human
,User_Department, User_SubDepartment, User_Region
,Priority, Record_Status, Process_ID, Process_TS
)
in
(
select
 artPI, SiteID, UserName_Pattern, User_Bucket, Is_Discrete_Human
,User_Department, User_SubDepartment, User_Region
,Priority, Record_Status, Process_ID, Process_TS
from coat_tmp_deleteusers
);

drop table coat_tmp_deleteusers;

/* export a final reconcile step, since this was complex. */
Select tbl as ENDING_COUNTS, Expected_Value, cnt from
(
    Select cast('Distinct Users in DBQL' as varchar(50)) as tbl
          ,'no expectation - whatever it is' as Expected_Value
          ,Count(distinct UserName) cnt
          ,1 as ord
    from coat_dat_DBQL_Detail
        union all
    Select 'Distinct Records in USER', 'same as above, or greater', Count(distinct UserName_Pattern) , 2 as ord from "00902.coat_dim_user.coa.csv" where artpi=4
        union all
    Select 'DBQL Records Not Bucketed', 'zero', Count(*) , 3 as ord from coat_dat_DBQL_Detail
    where UserName not in(Select UserName_Pattern from "00902.coat_dim_user.coa.csv" where artpi=4)
        union all
    Select 'Duplicates in USER', 'zero', count(*) , 4 as ord from
    (Select 1 as nm from "00902.coat_dim_user.coa.csv" where artpi=4
    qualify Row_Number()over(partition by UserName_Pattern order by UserName_Pattern) <>1) a
) a
order by ord;











/*
------- DAT_CONCURRENCY
------------------------------------
This will likely be the second longest-running query of this process, due to
the BETWEEN time statements below.  That said, in most cases it should only
take 3-5 minutes, WLM and other platform work not withstanding.

Concurrency is a point-in-time metric, not aggregated over time. This
process takes 4 point-in-time measurements per hour, aka every 15 minutes.
You can do more (say, every 5 minute) but understand, it is a resource-intense
operation, so approach modifications carefully.

The logic itself measures requests, aka multi-SQL requests are measured as one
query.  This should be fine, as multi-SQL requests operate sequentially, so
from a point-in-time concurrency measure, it is identical.

The join logic has several nested AND/OR.  A few reasons:
  - DQBL table is partitioned by LogDate...  that section only exists
    to provide partition elimination
  - the match-up between DIM_Time and DAT_DBQL happens with LogTS, in two ways:
    - LogTS between first query start, and final transmit or query complete
      this is done, as the database is still committing resources to the
      query right up until the last record is sent.
    - OR LogTS == Query Start TS == Final Query Complete TS
      this is done to capture zero-second tactical queries that happen to
      land on that point-in-time sample.   Often tactical will be so fast
      that logging tables will not have TS precision, and they appear as
      zero-second.  In very heavy tactical workloads, this can  result in
      under-reporting concurrency by 1 or 2.
*/
create volatile table coat_dat_concurrency as
(
    Select
     dbql.LogDate
    ,dbql.LogHour
    ,tmr.LogTS
    ,Sum(dbql.Query_Count) as  Concurrency
    from coat_dat_DBQL_detail as dbql
    join coat_dim_time_dayhour as tmr
      on (   dbql.LogDate = tmr.LogDate
          or dbql.LogDate = tmr.LogDate-1 )
     and (tmr.LogTS between dbql.Request_Start_Time and dbql.Request_Complete_Time
          or (tmr.LogTS = dbql.Request_Start_Time and
              tmr.LogTS = dbql.Request_Complete_Time)
          )
    group by 1,2,3
) with data
no primary index
on commit preserve rows;

/*{{save:{siteid}.coat_dat_concurrency.coa.csv}}*/
Select * from coat_dat_concurrency order by LogTS;














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

create volatile Table coat_dat_DBQL  as
(
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
    ,coalesce(app.App_Bucket, 'Unknown') as App_Bucket
    ,coalesce(app.Use_Bucket, 'Unknown') as Use_Bucket
    ,coalesce(stm.Statement_Bucket, 'Unknown') as Statement_Bucket
    ,usr.User_Bucket
    ,usr.Is_Discrete_Human
    ,dbql.WDName as Workload_Name
    ,usr.User_Department
    ,usr.User_SubDepartment
    ,usr.User_Region

    ,case when dbql.StatementType = 'Select'
            and dbql.AppID not in ('TPTEXP', 'FASTEXP')
            and dbql.Runtime_AMP_Sec < 1
            and dbql.NumOfActiveAMPs < dbql.Total_AMPs
          then 'Tactical'
          else 'Non-Tactical'
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

    From coat_dat_DBQL_detail as dbql

    join coat_dat_cpu_seconds as cpumax
      on dbql.LogDate = cpumax.LogDate
     and dbql.LogHour = cpumax.LogHour

    join "00900.coat_dim_app.coa.csv" as app
      on app.ArtPI=2
     and dbql.AppID = app.AppID

    join coat_dim_Statement stm
      on stm.ArtPI=3
     and dbql.StatementType = stm.StatementType

    join "00902.coat_dim_user.coa.csv" usr
      on usr.ArtPI=4
     and dbql.UserName = usr.UserName_Pattern

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
        ,Workload_Name
        ,Query_Type

) with data
no primary index
on commit preserve rows;

/*{{save:{siteid}.FINAL_DAT_DBQL_Record_Count.coa.csv}}*/
Select count(*) as FINAL_DAT_DBQL_Record_Count from coat_dat_DBQL;

/*{{save:{siteid}.coat_dat_DBQL.coa.csv}}*/
Select * from coat_dat_DBQL order by LogTS, User_Bucket, App_Bucket;
