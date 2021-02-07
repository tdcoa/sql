
-- COLLECT SITE ID, ACCOUNT, OTHER BASIC STUFF
/*{{save:bq_intro.csv}}*/
select '{siteid}' as Site_ID
,'{customer_name}' as Customer_Name
,case when InfoKey='VERSION' then infodata end as Database_Version
,USER
,Spool_GB
,current_timestamp(0) as RunTS
,cast(cast(RunTS as DATE format 'mmmmbddbyyyy')as varchar(20)) as FullDate
,'{your_name}' as Your_Name
,'{your_title}' as Your_Title
from dbc.dbcinfo
cross join (select trim(cast(SpoolSpace/1e9 as INT)(varchar(64))) as Spool_GB from dbc.users where username = USER) sp
where Database_Version is not null ;


-- ACTIVE AND TOTAL USERS
/*{{save:bq_user_counts.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast(count(*) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Users"
,cast(cast(sum(Active_Flag) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Active Users"
,cast(cast(count(case when User_Bucket = 'TDInternal' then NULL else username end) as format'ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Users, less Internal DBs"
,cast(cast(sum(case when User_Bucket = 'TDInternal' then NULL else Active_Flag end) as format'ZZZ,ZZZ,ZZ9') as varchar(32)) as "Active Users, less Internal DBs"
from dim_user ;


-- APPLICATION COUNTS
/*{{save:bq_appid_counts.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast(count(*) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Applications"
,cast(cast(count(distinct App_Bucket) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total App Buckets"
,cast(cast(count(case when App_Bucket <> 'Unknown' then AppID end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Recognized Applications"
,cast(cast(average(Request_Cnt)/(DATE-1 - DATE-15) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Average Requests per Application per Day"
from dim_App ;


-- CONCURRENCY COUNTS
/*{{save:bq_concurrency.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast(avg(Concurrency_Avg) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Concurrency Average" --2
,cast(cast(avg(Concurrency_80Pctl) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Concurrency 80th Percentile"
,cast(cast(avg(Concurrency_95Pctl) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Concurrency 95th Percentile"
,cast(cast(max(Concurrency_Peak) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Concurrency Peak" --5
from concurrency ;


-- DBQL CORE QUERY COUNTS
/*{{save:bq_query_counts.csv}}*/
select
 '{siteid}'  as Site_ID
,cast(cast(LogDayCnt as format 'ZZZ,ZZ9') as varchar(32)) as "Day Count"
,cast(cast(TotalQryCnt as BigInt format 'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Query Count"
,cast(cast(AvgQryPerSecond as Integer format 'ZZZ,ZZZ,ZZ9') as varchar(32)) as "Queries per Second"
,cast(cast(AvgQryPerDay as Integer format 'ZZZ,ZZZ,ZZ9') as varchar(32)) as "Queries per Day" --5
,cast(cast(AvgMilQryPerDay as Decimal(18,1) format 'ZZZ,ZZZ,ZZ9.9') as varchar(32)) as "Queries per Day (M)" --6
,cast(cast(AvgQryPerMonth as Decimal(18,1) format 'ZZZ,ZZZ,ZZ9.9') as varchar(32)) as "Queries per Month"
,cast(cast(AvgMilQryPerMonth as Decimal(18,1) format 'ZZZ,ZZZ,ZZ9.9') as varchar(32)) as "Queries per Month (M)" --8
,cast(cast(QryCntPerYear as BigInt format 'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Query Count per Year"
,cast(cast(MilQryCntPerYear as Decimal(18,1) format 'ZZZ,ZZZ,ZZ9.9') as varchar(32)) as "Query Count per Year (M)" --10
,cast(cast(BilQryCntPerYear as Decimal(18,1) format 'ZZZ,ZZZ,ZZ9.9') as varchar(32)) as "Query Count per Year (B)"
,cast(cast(TotalTacticalCntPerDay as BigInt format 'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Tactical Query Count per Day"
,cast(cast(TotalTacticalCntPerDayMil as Decimal(18,1) format 'ZZZ,ZZZ,ZZZ,ZZ9.9') as varchar(32)) as "Tactical Query Count Per Day (M)"
,cast(cast(TacticalPct as Decimal(9,2) format 'ZZ9.9') as varchar(32)) as "Tactical % of Total Queries"
,cast(cast(AvgRunTimeSec as Decimal(9,2) format 'Z,ZZZ,ZZ9.99') as varchar(32)) as "Average Runtime Seconds" --15
,cast(cast(MedianRunTimeSec as Decimal(9,2) format 'Z,ZZZ,ZZ9.99') as varchar(32)) as "Median Runtime Seconds"
from
(
select
 count(distinct cast(LogTS as char(10))) as LogDayCnt
,sum(Query_Cnt) AS TotalQryCnt
,TotalQryCnt / LogDayCnt AS AvgQryPerDay
,AvgQryPerDay  / 1e6 AS AvgMilQryPerDay
,AvgQryPerDay * 30 AS AvgQryPerMonth
,AvgMilQryPerDay * 30 AS AvgMilQryPerMonth
,AvgQryPerDay / (24*60*60) AS AvgQryPerSecond
,TotalQryCnt * 365 / LogDayCnt AS QryCntPerYear
,QryCntPerYear / 1e6 AS MilQryCntPerYear
,QryCntPerYear / 1e9 AS BilQryCntPerYear
,sum(Query_Tactical_Cnt) AS TotalTacticalCnt
,sum(Query_Tactical_Cnt)/ LogDayCnt  AS TotalTacticalCntPerDay
,cast(TotalTacticalCntPerDay as decimal(18,2)) / 1e6  AS TotalTacticalCntPerDayMil
,(cast(TotalTacticalCnt as decimal(18,4)) / TotalQryCnt) * 100 AS TacticalPct
,sum(Runtime_Total_Sec) / TotalQryCnt AS AvgRunTimeSec
,median(Runtime_Total_Sec) as MedianRuntimeSec
from dbql_core_hourly
) d1 ;


-- QUERIES PER APPLICATION
/*{{save:bq_appid_detail.csv}}*/
select
 case when app.App_Bucket='Unknown' then app.AppID else app.App_Bucket end as App_Group
,(DATE-1) - (DATE-15)(INT) as DayCount
,cast(cast(sum(dbql.Query_Cnt)/DayCount as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Queries"
,cast(cast(sum(dbql.Returned_Row_Cnt)/DayCount as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Fetched Rows"
,zeroifnull(cast(cast("Total Fetched Rows" / nullifzero("Total Queries") as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))) as "Rows Per Query"
from dbql_core_hourly dbql
join dim_app as app
  on dbql.AppID = app.AppID
group by 1
Order by "Rows per Query" desc ;


-- FOR GRAPHING:  Queries per Day
/*{{save:bq--daily_query_throughput.csv}}*/
/* { {vis:bq--daily_query_throughput.csv}} */
select
 cast(LogTS as char(10)) as "Log Date"
,cast(cast(sum(Query_Cnt) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Queries--#27C1BD"
,cast(cast(sum(Query_Tactical_Cnt) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Tactical Queries--#636363"
from dbql_core_hourly
group by cast(LogTS as char(10))
order by 1 ;


-- DISK SPACE
/*{{save:bq_diskspace.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast((MaxPermGB) as Decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Max Available Space (GB)"
,cast(cast((MaxPermGB)/1e3 as Decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Max Available Space (TB)" --3
,cast(cast((CurrentPermGB) as Decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Used Space (GB)"
,cast(cast((CurrentPermGB)/1e3 as Decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Used Space (TB)" --5
,cast(cast((FilledPct)*100 as Decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Filled Percent"
from db_objects_cds
where DBName = '*** Total ***'
qualify LogDate = max(LogDate)over() ;
