/* pulls cpu, io, query count, query complexity, runtime,
   and error count, per user and user alignment.
   Generates UserHash as proxy for UserName, so it can be
   uploaded to transcend.

   Parameters:
     - siteid:     {siteid}
     - startdate:  {startdate}
     - enddate:    {enddate}
     - dbqlogtbl:  {dbqlogtbl}
*/


/* define dates for number of months  */
/* rules:
   - full weeks (7 days) only, no partial weeks (having)
       EXCEPT for current week ONLY IF there are less than 7 days in original request
   - this month: partial month allowed (qualify)
   - prev months: full months only (qualify)
   - Month start/end measured on ISO calendar
   in other words, you will almost always get fewer dates than you asked for
*/
create volatile table top_user_dates as (
  Select
   cast(YearNumber_of_Calendar(calendar_date,'ISO') as int) as year_num
  ,cast(MonthNumber_of_Year   (calendar_date,'ISO') as int) as month_num
  ,cast(WeekNumber_of_Month   (calendar_date,'ISO') as int) as week_num
  ,cast((year_num*1000)+(month_num*10)+(week_num) as int) as WeekID
  ,cast(WeekID/10 as int) as MonthID
  ,case when cast(MonthNumber_of_Year(DATE-1,'ISO') as int) = month_num then 1 else 0  end as month_cur
  ,case when cast(WeekNumber_of_Month(DATE-1,'ISO') as int) = week_num and month_cur = 1 then 1 else 0  end as week_cur
  ,min(Calendar_Date) as MinDate
  ,max(Calendar_Date) as MaxDate
  ,count(Calendar_Date) as Actual_Date_Cnt
  ,({enddate} - ({startdate})) as Orig_Date_Cnt
  from sys_calendar.calendar
  where calendar_date between {startdate} and {enddate}
  group by year_num, month_num, week_num
  qualify (min(week_num)over(partition by month_num) = 1 or month_cur=1)
  having (count(Calendar_Date)=7 or (week_cur = 1 and Orig_Date_Cnt < 7))
) with data no primary index on commit preserve rows
;

Collect stats on top_user_dates column (WeekID)
;

/*{{save:top_user_dates.csv}}*/
Select * from top_user_dates
;


/* LONG PULL: pre-aggregate from dbql, to reduce spool-outs
 */
Create Volatile Table Top_Users_DBQL_preagg  as(
    Select UserName
    ,LogDate
    ,zeroifnull(sum(cast(Statements as BigInt))) as Query_Cnt
    ,zeroifnull(avg(cast(NumSteps as Int))) as Query_Complexity_Score
    ,zeroifnull(sum(cast(ParserCPUTime+AMPCPUTime as Decimal(18,2)))) as CPU_Sec
    ,zeroifnull(sum(cast(ReqIOKB/1e6 as Decimal(18,0)))) as IOGB
    ,zeroifnull(sum(cast(TotalFirstRespTime as Decimal(18,6)))) as Runtime_Sec
    ,zeroifnull(sum(cast((case when dbql.ErrorCode not in(0,3158) then 1 else 0 end) as decimal(9,0)))) as Error_Cnt
    from {dbqlogtbl} /* pdcrinfo.dbqlogtbl_hst */ as dbql
    where LogDate between {startdate} and {enddate}
    Group by UserName, LogDate
) with data
  no primary index
  on commit preserve rows
;

/*{{save:users_active.csv}}*/
select cast(cast(count(distinct UserName) as BigInt format'ZZZ,ZZZ,ZZZ,ZZZ') as varchar(32)) as Active_User_Cnt
,count(distinct LogDate) as Days_Cnt
from  Top_Users_DBQL_preagg
;


Create Volatile Table Top_Users_DBQL  as(
    Select UserName, dt.WeekID, dt.MonthID
    ,zeroifnull(sum(Query_Cnt)) as Query_Cnt
    ,zeroifnull(avg(Query_Complexity_Score)) as Query_Complexity_Score
    ,zeroifnull(sum(CPU_Sec)) as CPU_Sec
    ,zeroifnull(sum(IOGB)) as IOGB
    ,zeroifnull(sum(Runtime_Sec)) as Runtime_Sec
    ,zeroifnull(sum(Error_Cnt)) as Error_Cnt
    from Top_Users_DBQL_preagg as dbql
    join top_user_dates as dt
      on LogDate between MinDate and MaxDate
    Group by UserName, rollup(MonthID, WeekID)
) with data
  primary index(UserName)
  on commit preserve rows
;

drop table top_user_dates
;
drop table Top_Users_DBQL_preagg
;

collect stats on Top_Users_DBQL column(UserName)
;


Create Volatile Table Top_Users_Rank  as(
  Select '{siteid}' as Site_ID, a.*
  ,rank()over(partition by MonthID, WeekID order by Combined_Score asc) as Total_Rank
  from(
      Select WeekID, MonthID, u.UserName
      ,u.UserHash, u.User_Bucket
      ,u.User_Department, u.User_SubDepartment, u.User_Region
      ,Query_Cnt
      ,rank()over(partition by MonthID, WeekID order by Query_Cnt desc) as Query_Cnt_Rank
      ,Query_Complexity_Score
      ,rank()over(partition by MonthID, WeekID order by Query_Complexity_Score desc) as Query_Complexity_Score_Rank
      ,CPU_Sec
      ,rank()over(partition by MonthID, WeekID order by CPU_Sec desc) as CPU_Sec_Rank
      ,IOGB
      ,rank()over(partition by MonthID, WeekID order by IOGB desc) as IOGB_Rank
      ,Runtime_Sec
      ,rank()over(partition by MonthID, WeekID order by Runtime_Sec desc) as Runtime_Sec_Rank
      ,Error_Cnt
      ,rank()over(partition by MonthID, WeekID order by Error_Cnt desc) as Error_Cnt_Rank
      ,Query_Cnt_Rank + Query_Complexity_Score_Rank + CPU_Sec_Rank + IOGB_Rank + Runtime_Sec_Rank as Combined_Score
      from Top_Users_DBQL as d
      join Dim_User as u
        on d.UserName = u.UserName
    ) a
) with data
  primary index(UserName)
  on commit preserve rows
;


drop table Top_Users_DBQL
;

collect stats on Top_Users_Rank column(UserName)
;

/*{{save:top_users.csv}}*/
/*{{load:{db_stg}.stg_dat_top_users}}*/
/*{{call:{db_coa}.sp_dat_top_users('v1')}}*/
/*{{vis:top_users.csv}}*/
Select * from Top_Users_Rank
;


/*{{save:top_users_rank1.csv}}*/
select cast('Query_Cnt' as varchar(128)) as Category
,cast(max(case when Query_Cnt_Rank=1 then UserName || ' / '
     || User_Bucket || ' ('
     || trim(cast(Query_Cnt as BIGINT format'ZZZ,ZZZ,ZZZ,ZZZ')) || ')'
     else '' end) as varchar(128)) as "Rank #1 User"
from Top_Users_Rank  where WeekId is null and MonthID is null
UNION ALL
select cast('Query_Complexity' as varchar(128)) as Metric
,max(case when Query_Complexity_Score_Rank=1 then UserName || ' / ' || User_Bucket
          else '' end) as Query_Complexity
from Top_Users_Rank  where WeekId is null and MonthID is null
UNION ALL
select cast('CPU_Sec' as varchar(128)) as Metric
,max(case when CPU_Sec_Rank=1 then UserName || ' / ' || User_Bucket || ' (' || trim(cast(CPU_Sec as BIGINT format'ZZZ,ZZZ,ZZZ,ZZZ')) || ')'
          else '' end) as CPU_Sec
from Top_Users_Rank  where WeekId is null and MonthID is null
UNION ALL
select cast('IOGB' as varchar(128)) as Metric
,max(case when IOGB_Rank=1 then UserName || ' / ' || User_Bucket || ' (' || trim(cast(IOGB as BIGINT format'ZZZ,ZZZ,ZZZ,ZZZ')) || ')'
          else '' end) as IOGB
from Top_Users_Rank  where WeekId is null and MonthID is null
UNION ALL
select cast('Runtime_Sec' as varchar(128)) as Metric
,max(case when Runtime_Sec_Rank=1 then UserName || ' / ' || User_Bucket || ' (' || trim(cast(Runtime_Sec as BIGINT format'ZZZ,ZZZ,ZZZ,ZZZ')) || ')'
          else '' end) as Runtime_Sec
from Top_Users_Rank  where WeekId is null and MonthID is null
UNION ALL
select cast('Error_Cnt' as varchar(128)) as Metric
,max(case when Error_Cnt_Rank=1 then UserName || ' / ' || User_Bucket || ' (' || trim(cast(Error_Cnt as BIGINT format'ZZZ,ZZZ,ZZZ,ZZZ')) || ')'
          else '' end) as Error_Cnt
from Top_Users_Rank  where WeekId is null and MonthID is null
UNION ALL
select cast('Total_Score' as varchar(128)) as Metric
,max(case when Total_Rank=1 then UserName || ' / ' || User_Bucket || ' (' || trim(cast(Combined_Score as BIGINT format'ZZZ,ZZZ,ZZZ,ZZZ')) || ')'
          else '' end) as Total_Score
from Top_Users_Rank  where WeekId is null and MonthID is null
;
