
-- COLLECT SITE ID, ACCOUNT, OTHER BASIC STUFF
/*{{save:bq--intro.csv}}*/
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
/*{{save:bq--user_counts.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast(count(*) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Users"
,cast(cast(sum(Active_Flag) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Active Users"
,cast(cast(count(case when User_Bucket = 'TDInternal' then NULL else username end) as format'ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Users, less Internal DBs"
,cast(cast(sum(case when User_Bucket = 'TDInternal' then NULL else Active_Flag end) as format'ZZZ,ZZZ,ZZ9') as varchar(32)) as "Active Users, less Internal DBs"
from dim_user ;


-- APPLICATION COUNTS
/*{{save:bq--appid_counts.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast(count(*) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Applications"
,cast(cast(count(distinct App_Bucket) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total App Buckets"
,cast(cast(count(case when App_Bucket <> 'Unknown' then AppID end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Recognized Applications"
,cast(cast(average(cast(Request_Cnt as bigint))/cast(({enddate}) - ({startdate}) as BigInt) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Average Requests per Application per Day"
from dim_App ;



-- CONCURRENCY COUNTS
/*{{save:bq--concurrency.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast(avg(Concurrency_Avg) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Concurrency Average" --2
,cast(cast(avg(Concurrency_80Pctl) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Concurrency 80th Percentile"
,cast(cast(avg(Concurrency_95Pctl) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Concurrency 95th Percentile"
,cast(cast(max(Concurrency_Peak) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Concurrency Peak" --5
from concurrency ;


-- NEW: SubSecond QueryCount
/*{{save:bq--subsecond_query.csv}}*/
Select
   '{siteid}'  as Site_ID
  ,count(distinct LogDate) as DayCount
  ,sum(qrycnt_in_runtime_0000_0001) / DayCount as SubSecond_Queries
  ,cast(cast(SubSecond_Queries as BigInt format 'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as SubSecond_Queries_Formatted
  ,sum(qrycnt_in_runtime_0000_0001
     + qrycnt_in_runtime_0001_0005
     + qrycnt_in_runtime_0005_0010
     + qrycnt_in_runtime_0010_0030
     + qrycnt_in_runtime_0030_0060
     + qrycnt_in_runtime_0060_0300
     + qrycnt_in_runtime_0300_0600
     + qrycnt_in_runtime_0600_1800
     + qrycnt_in_runtime_1800_3600
     + qrycnt_in_runtime_3600_plus) / DayCount as Total_Queries
  ,cast(cast(Total_Queries as BigInt format 'ZZZZ,ZZZZ,ZZZ,ZZ9') as varchar(32)) as Total_Queries_Formatted
  ,cast(cast(
    cast(SubSecond_Queries as decimal(32,4)) / cast(Total_Queries as decimal(32,4)) * 100.00
   as decimal(32,2)) as varchar(32)) as  SubSecond_Queries_Pct
from dbql_core_breakout
;



-- FOR GRAPHING:  Queries per Day
/*{{save:bq--daily_query_throughput.csv}}*/
/*{{vis:bq--daily_query_throughput.csv}}*/
Select
   LogDate as "Log Date"
  ,sum(qrycnt_in_runtime_0000_0001) as "SubSecond Queries--#636363"
  ,sum(qrycnt_in_runtime_0000_0001
     + qrycnt_in_runtime_0001_0005
     + qrycnt_in_runtime_0005_0010
     + qrycnt_in_runtime_0010_0030
     + qrycnt_in_runtime_0030_0060
     + qrycnt_in_runtime_0060_0300
     + qrycnt_in_runtime_0300_0600
     + qrycnt_in_runtime_0600_1800
     + qrycnt_in_runtime_1800_3600
     + qrycnt_in_runtime_3600_plus) as "Total Queries--#27C1BD"
from dbql_core_breakout
group by LogDate
order by LogDate
;

-- DBQL CORE QUERY COUNTS
/*{{save:bq--query_counts.csv}}*/
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
,cast(cast(TacticalPct as Decimal(9,2) format 'ZZ9.9') as varchar(32)) as "Tactical % of Total Queries" --14
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
/*{{save:bq--appid_detail.csv}}*/
select
 app.App_Bucket
,max(cast(cast(LogTS as timestamp(0)) as DATE)) -
 min(cast(cast(LogTS as timestamp(0)) as DATE))+1 as DayCount
,cast(cast(sum(dbql.Query_Cnt)/nullifzero(DayCount) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Queries"
,cast(cast(sum(dbql.Returned_Row_Cnt)/ nullifzero(DayCount) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Fetched Rows"
,cast(cast(zeroifnull("Total Fetched Rows" / nullifzero("Total Queries")) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Rows Per Query"
,'nothing' as debug_string_please_ignore
from dbql_core_hourly dbql
join dim_app as app
  on dbql.AppID = app.AppID
group by 1
Order by cast("Rows per Query" as INT) desc ;



-- DISK SPACE
/*{{save:bq--diskspace.csv}}*/
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


-- OBJECT COUNTS
/*{{save:bq--object_counts.csv}}*/
Select
 '{siteid}' as Site_ID
,cast(cast(sum(TableCount) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Table Count"
,cast(cast(sum(ViewCount) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "View Count"
,cast(cast(sum(MacroCount+"SP&TrigCount"+UDObjectCount) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Program Count"
,cast(cast(sum(IndexCount+OtherCount) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Other Count"
from db_objects_counts where DBName <>  '*** Total ***' -- there is a RollUp in DB_Objects
;


-- COLUMN FORMATS
/*{{save:bq--column_formats.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast(sum(case when FormatInd = 'Y' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Formatted Columns"
,cast(cast(sum(case when ColumnCategory = 'Interval' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Type Interval"
,cast(cast(sum(case when ColumnCategory = 'Period' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Type Period"
,cast(cast(sum(case when ColumnCategory = 'Number' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Type Number" --5
,cast(cast(sum(case when ColumnCategory = 'BLOB' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Type BLOB"
,cast(cast(sum(case when ColumnCategory = 'CLOB' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Type CLOB"
,cast(cast(sum(case when ColumnCategory like any('XML%','_SON%','Avro') then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Type XML/JSON"
,cast(cast(sum(case when ColumnCategory like 'Geosp%' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Type GeoSpatial" --9
from column_types ;


-- CONSTRAINTS
/*{{save:bq--constraints.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast(sum(case when ConstraintType in('Primary Key','Unique') then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Unique PI Constraint"
,cast(cast(sum(case when ConstraintType = 'Primary Key' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Primary Key Constraint"
,cast(cast(sum(case when ConstraintType = 'Default' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Column Default"
,cast(cast(sum(case when ConstraintType = 'Foreign Key' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Foreign Key Constraint" --5
,cast(cast(sum(case when ConstraintType = 'Column Constraint' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Column Constraint"
,cast(cast(sum(case when ConstraintType = 'Table Constraint' then 1 else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Table Constraint"
from constraint_details ;


-- CALCULATE DML COUNTS (INS/UPD/DEL/MERGE) PER TABLE
-- Note: LogDate == StartTime(DATE), not CollectTimeStamp(DATE)
--       but dbqlobjtbl does not have StartTime, so join on CollectTimeStamp(DATE)
---- DBC SPECIFIC
create volatile table dml_count_per_table as
(
  Select l.StartTime(DATE) as LogDate, o.ObjectDatabaseName as DatabaseName, o.ObjectTableName as TableName, count(*) as Request_Count
  from DBC.dbqlogtbl as l
  join DBC.dbqlobjtbl as o
    on l.ProcID = o.ProcID and l.QueryID = o.QueryID
   and l.CollectTimeStamp(DATE) = o.CollectTimeStamp(DATE)
  where l.StartTime(DATE) between {startdate} and {enddate}
    and l.StatementType in ('Insert','Update','Delete','Merge')
    and o.ObjectTableName is not null
  group by 1,2,3
) with data
primary index(LogDate, TableName)
on commit preserve rows ;

-- NUMBER OF TABLES THAT EXCEED 1500 INSERTS/UPDATES/DELETES PER DAY (Graph)
/*{{save:bq--tablecount_over_1500_dml.csv}}*/
/*{{vis:bq--tablecount_over_1500_dml.csv}}*/
Select cast(cast(LogDate as date format 'yyyy-mm-dd') as char(10)) AS LogDate
,Count(TableName) as "Table Count--#27C1BD"
from dml_count_per_table
where Request_Count > 1500
group by 1
order by 1 ;


-- OVERALL COUNT OF TABLES THAT EXCEED 1500 DML PER DAY
/*{{save:bq--tablecount_over_1500_dml_average.csv}}*/
Select
 '{siteid}' as Site_ID
,cast(cast(avg(TableCount) as INT format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as TableCount
from (
    Select LogDate, Count(TableName) as TableCount
    from dml_count_per_table where Request_Count > 1500
    group by 1
) as a ;


-- DATABASES WITH MOST DML PER TABLE
/*{{save:bq--databases_most_dml_tables.csv}}*/
Select DatabaseName
,cast(cast(avg(cast(Request_Count as BigInt)) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Avg DML Request Count per Table"
from dml_count_per_table
group by 1
order by avg(cast(Request_Count as BigInt)) desc ;


-- FORMATTED COLUMNS
/*{{save:bq--column_format_state.csv}}*/
SELECT '{siteid}' as Site_ID
,cast(cast(sum(CASE WHEN ColumnFormat IS NOT NULL THEN 1 ELSE 0 END) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "FORMATTED"
,cast(cast(sum(CASE WHEN ColumnFormat IS     NULL THEN 1 ELSE 0 END) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "UNFORMATTED"
from DBC.COlumnsV group by 1 ;


-- DATA TRANSFER NUMBERS
/*{{save:bq--data_transfer.csv}}*/
/*{{vis:bq--data_transfer.csv}}*/
SELECT cast(cast(TheDate as date format 'yyyy-mm-dd') as char(10)) AS LogDate
      ,cast(SUM(HostReadKB)*1e3 as bigint)  as "Inbound Bytes--#27C1BD"
      ,cast(SUM(HostWriteKB)*1e3 as bigint) as "Outbound Bytes--#636363"
FROM dbc.ResUsageSPMA
WHERE TheDate BETWEEN {startdate} and {enddate}
GROUP BY LogDate ORDER BY LogDate;


-- JOIN FREQUENCY
-- broken into steps for efficiency
create volatile table vt_queryid_by_joincount as
(
    Select
     QueryID, CollectTimeStamp(DATE) as LogDate, Count(distinct ObjectTableName) as JoinCount
    from dbc.DBQLObjTbl
    where ObjectColumnName is null
      and ObjectTableName is not null
      and ObjectType in ('Tab', 'Viw')
      and LogDate BETWEEN {startdate} and {enddate}
    group by 1,2
) with data
primary index (QueryID, LogDate)
on commit preserve rows;

collect stats on vt_queryid_by_joincount column(QueryID, LogDate);

create volatile table vt_query_n_cpu_by_joincount as
(
  Select
   CASE WHEN JoinCount <= 5 THEN (JoinCount (FORMAT 'Z9') (CHAR(2))) ELSE ' 6+' END as Join_Label
  ,cast(cast(count(*) as BigInt format 'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as Request_Count
  ,cast(cast(sum(dbql.ParserCPUTime+dbql.AMPCPUtime) as decimal(32,2) format 'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as CPU_Sec
  ,count(distinct j.LogDate) as DateCount
  from dbc.dbqlogtbl as dbql
  join vt_queryid_by_joincount as j
    on dbql.StartTime(DATE) = j.LogDate
   and dbql.QueryID = j.QueryID
  group by 1
) with data no primary index
on commit preserve rows;


/*{{save:bq--join_frequency_log.csv}}*/
/*{{vis:bq--join_frequency_log.csv}}*/
Select
 join_label || case when join_label=1 then ' Table' else ' Tables' end  as "Number of Tables" --xaxis
,Request_Count as "Number of Queries (LOG Scale)--#27C1BD" --bars
,cast(cast(CPU_Sec / sum(CPU_Sec)over()*100 as decimal(9,2)) as varchar(16))  as "CPU Consumed %--#636363" --line
from vt_query_n_cpu_by_joincount order by 1 asc ;


/*{{save:bq--join_frequency.csv}}*/
/*{{vis:bq--join_frequency.csv}}*/
Select
 join_label || case when join_label=1 then ' Table' else ' Tables' end  as "Number of Tables" --xaxis
,Request_Count as "Number of Queries--#27C1BD" --bars
,cast(cast(CPU_Sec / sum(CPU_Sec)over()*100 as decimal(9,2)) as varchar(16))  as "CPU Consumed %--#636363" --line
from vt_query_n_cpu_by_joincount order by 1 asc ;


/*{{save:bq--join_frequency_horz.csv}}*/
Select
 cast(cast(sum(case when join_label=1 then cast(Request_Count as bigint) else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join1_Mrequest_count --1
,cast(cast(sum(case when join_label=2 then cast(request_count as bigint) else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join2_Mrequest_count --2
,cast(cast(sum(case when join_label=3 then cast(request_count as bigint) else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join3_Mrequest_count --3
,cast(cast(sum(case when join_label=4 then cast(request_count as bigint) else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join4_Mrequest_count --4
,cast(cast(sum(case when join_label=5 then cast(request_count as bigint) else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join5_Mrequest_count --5
,cast(cast(sum(case when join_label=6 then cast(request_count as bigint) else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join6_Mrequest_count --6
,cast(cast(sum(cast(Request_Count as bigint))/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as total_Mrequest_count --7
,cast(cast(cast(sum(case when join_label=1 then cast(Request_Count as bigint) else 0 end) as decimal(32,4))
/cast(sum(cast(Request_Count as bigint)) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join1_request_pct --8
,cast(cast(cast(sum(case when join_label=2 then cast(Request_Count as bigint) else 0 end) as decimal(32,4))
/cast(sum(cast(Request_Count as bigint)) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join2_request_pct --9
,cast(cast(cast(sum(case when join_label=3 then cast(Request_Count as bigint) else 0 end) as decimal(32,4))
/cast(sum(cast(Request_Count as bigint)) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join3_request_pct --10
,cast(cast(cast(sum(case when join_label=4 then cast(Request_Count as bigint) else 0 end) as decimal(32,4))
/cast(sum(cast(Request_Count as bigint)) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join4_request_pct --11
,cast(cast(cast(sum(case when join_label=5 then cast(Request_Count as bigint) else 0 end) as decimal(32,4))
/cast(sum(cast(Request_Count as bigint)) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join5_request_pct --12
,cast(cast(cast(sum(case when join_label=6 then cast(Request_Count as bigint) else 0 end) as decimal(32,4))
/cast(sum(cast(Request_Count as bigint)) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join6_request_pct --13

,cast(cast(sum(case when join_label=1 then cpu_sec else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join1_Mcpu_sec --14
,cast(cast(sum(case when join_label=2 then cpu_sec else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join2_Mcpu_sec --15
,cast(cast(sum(case when join_label=3 then cpu_sec else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join3_Mcpu_sec --16
,cast(cast(sum(case when join_label=4 then cpu_sec else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join4_Mcpu_sec --17
,cast(cast(sum(case when join_label=5 then cpu_sec else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join5_Mcpu_sec --18
,cast(cast(sum(case when join_label=6 then cpu_sec else 0 end)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as join6_Mcpu_sec --19
,cast(cast(sum(cpu_sec)/1e6 as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32))||'M' as total_Mcpu_sec --20
,cast(cast(cast(sum(case when join_label=1 then cpu_sec else 0 end) as decimal(32,4))
/cast(sum(cpu_sec) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join1_cpu_pct --21
,cast(cast(cast(sum(case when join_label=2 then cpu_sec else 0 end) as decimal(32,4))
/cast(sum(cpu_sec) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join2_cpu_pct --22
,cast(cast(cast(sum(case when join_label=3 then cpu_sec else 0 end) as decimal(32,4))
/cast(sum(cpu_sec) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join3_cpu_pct --23
,cast(cast(cast(sum(case when join_label=4 then cpu_sec else 0 end) as decimal(32,4))
/cast(sum(cpu_sec) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join4_cpu_pct --24
,cast(cast(cast(sum(case when join_label=5 then cpu_sec else 0 end) as decimal(32,4))
/cast(sum(cpu_sec) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join5_cpu_pct --25
,cast(cast(cast(sum(case when join_label=6 then cpu_sec else 0 end) as decimal(32,4))
/cast(sum(cpu_sec) as decimal(32,4)) *100 as decimal(9,2)) as varchar(16))||'%' as join6_cpu_pct --26
,max(DateCount)(INT) as DateCount -- 27
from vt_query_n_cpu_by_joincount ;


drop table vt_queryid_by_joincount;


-- CURRENT TABLES OVER 10GB IN SIZE
create volatile table tables_size_10g as
(
  SELECT
      DataBaseName
     ,TableName
     ,cast(Sum(CurrentPerm/(2**30)) AS decimal(9,1)) CurrentPerm_GB
     ,cast(Sum(PeakPerm/(2**30)) AS decimal(9,1)) AS PeakPerm_GB
  FROM	Dbc.TableSizeV
  GROUP BY 1,2
  HAVING CurrentPerm_GB > 10
) with data
no primary index
on commit preserve rows
;

/*{{save:bq--10GB_table_count.csv}}*/
SELECT
    '{siteid}' as Site_ID
    ,cast(cast(coalesce(count(*), 0) as format 'ZZZ,ZZZ,ZZ9') as varchar(32))  AS tbl10GB
from tables_size_10g
;

/*{{save:bq--10GB_tables.csv}}*/
SELECT
    '{siteid}' as Site_ID
   ,DataBaseName
   ,TableName
   ,cast(cast(CurrentPerm_GB as format 'ZZZ,ZZZ,ZZ9.9') as varchar(32)) AS "CurrentPerm GB"
FROM tables_size_10g
ORDER BY CurrentPerm_GB DESC
;


-- OBJECT COUNT BY ATTRIBUTE:
create volatile table vt_table_kinds_by_database as
(
  SELECT
     COALESCE(DatabaseName, '') AS DatabaseName
    ,COALESCE(tk.Table_Bucket, 'Unknown - ' || t.TableKind) AS TableBucket
    ,COALESCE(tk.TableKind_Desc, 'Unknown - ' || t.TableKind) AS TableKindDesc
    ,CheckOpt AS MultisetInd
    ,GTTableCount
    ,ZEROIFNULL(ObjectCount) AS ObjectCount
  FROM
  (
   SELECT
     DatabaseName
    ,TableKind
    ,CheckOpt
    ,sum(case when CommitOpt <> 'N' then 1 else 0 end) as GTTableCount
    ,COUNT(*) AS ObjectCount
   FROM DBC.TablesV
   GROUP BY 1,2,3
  ) t
  FULL OUTER JOIN dim_tablekind as tk
  on t.TableKind = tk.TableKind
) with data
no primary index on commit preserve rows
;

/*{{save:bq--tablekind_by_database.csv}}*/
select
 '{siteid}' as Site_ID
 ,cast(cast(sum(case when TableBucket = 'Table' then ObjectCount else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Total Count"
 ,cast(cast(sum(case when TableKindDesc = 'Join Index' then ObjectCount else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Join Index Count"
 ,cast(cast(sum(case when TableKindDesc = 'Queue Table' then ObjectCount else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Queue Table Count"
 ,cast(cast(sum(case when TableBucket = 'Table' and MultiSetInd = 'N' then ObjectCount else 0 end) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "SET Table Count"
 ,cast(cast("SET Table Count" / "Total Count" * 100 as decimal(9,2) format 'ZZ9.99') as varchar(8))||'%' as "Set Table Pct"
 ,cast(cast(sum(GTTableCount) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Global Temp Table Count"
FROM vt_table_kinds_by_database
WHERE DatabaseName NOT IN  (select dbname from dim_tdinternal_databases)
;


-- MORE INDEX TYPE SQL
create volatile table vt_index_types_by_database as
(
 SEL
   COALESCE(Inds.DatabaseName, '') AS DatabaseName
  ,COALESCE(IK.IndexTypeDesc, 'Unknown - ' || Inds.IndexType) AS IndexTypeDesc
  ,COALESCE(IK.IndexTypeBucket, 'Unknown - ' || Inds.IndexType) AS IndexTypeBucket
  ,ZEROIFNULL(Inds.IndexCount) AS IndexCount
  FROM
  (
    SEL
       DatabaseName
      ,IndexType
      ,UniqueFlag
      ,COUNT(*) AS IndexCount
    FROM DBC.IndicesV
    GROUP BY 1,2,3
  ) Inds
  FULL OUTER JOIN dim_indextype AS IK
   ON IK.IndexType = Inds.IndexType
  AND IK.UniqueFlag = Inds.UniqueFlag
) with data
no primary index on commit preserve rows
;

/*{{save:bq--Index_summary.csv}}*/
select
 '{siteid}' as Site_ID
,CAST(CAST(SUM(CASE WHEN IndexTypeBucket = 'Primary Index' THEN IndexCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS UPINUPI
,CAST(CAST(SUM(CASE WHEN IndexTypeBucket = 'Partition' THEN IndexCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS PPI
,CAST(CAST(SUM(CASE WHEN IndexTypeBucket = 'Secondary Index' THEN IndexCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS SI
from vt_index_types_by_database
Where DatabaseName NOT IN  (select dbname from dim_tdinternal_databases)
;

-- BUILD FINAL PPTX DOCUMENT
/*{{pptx:!BigQuery_Migration.pptx}}*/
