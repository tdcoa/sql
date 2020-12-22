/* DBQL_CORE -- default SELECT statements that return rows from volatile tables.
   This is broken out so it can be overridden by other processes, as needed.

   Parameters:  none beyond normal config.yaml items...
                dates are constrained during CVT
*/

/*{{save:dat_query_counts.csv}}*/
select
 '{siteid}'  as Site_ID
,cast(cast(LogDayCnt as format 'ZZZ,ZZ9') as varchar(32)) as LogDayCnt
,cast(cast(TotalQryCnt as BigInt format 'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as TotalQryCnt
,cast(cast(AvgQryPerDay as Integer format 'ZZZ,ZZZ,ZZ9') as varchar(32)) as AvgQryPerDay
,cast(cast(AvgMilQryPerDay as Decimal(9,1) format 'ZZZ,ZZZ,ZZ9.9') as varchar(32)) as AvgMilQryPerDay
,cast(cast(AvgMilQryPerMonth as Decimal(9,1) format 'ZZZ,ZZZ,ZZ9.9') as varchar(32)) as AvgMilQryPerMonth
,cast(cast(AvgQryPerSecond as Integer format 'ZZZ,ZZZ,ZZ9') as varchar(32)) as AvgQryPerSecond
,cast(cast(QryCntPerYear as BigInt format 'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as QryCntPerYear
,cast(cast(BilQryCntPerYear as Decimal(9,1) format 'ZZZ,ZZZ,ZZ9.9') as varchar(32)) as BilQryCntPerYear
,cast(cast(TotalTacticalCnt as BigInt format 'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as TotalTacticalCnt
,cast(cast(TacticalPct as Decimal(5,1) format 'ZZ9.9') as varchar(32)) as TacticalPct
,cast(cast(AvgRunTimeSec as Decimal(9,2) format 'Z,ZZZ,ZZ9.99') as varchar(32)) as AvgRunTimeSec
from
(
select
 count(distinct cast(LogTS as char(10))) as LogDayCnt
,sum(Request_Cnt) AS TotalQryCnt
,TotalQryCnt / LogDayCnt AS AvgQryPerDay
,AvgQryPerDay  / 1e6 AS AvgMilQryPerDay
,AvgMilQryPerDay * 30 AS AvgMilQryPerMonth
,AvgQryPerDay / 3600 AS AvgQryPerSecond
,TotalQryCnt * 365 / LogDayCnt AS QryCntPerYear
,QryCntPerYear / 1e9 AS BilQryCntPerYear
,sum(Query_Tactical_Cnt) AS TotalTacticalCnt
,cast(TotalTacticalCnt as decimal(18,4)) / TotalQryCnt * 100 AS TacticalPct
,sum(Runtime_AMP_Sec) / TotalQryCnt AS AvgRunTimeSec
from dbql_core_hourly
) d1
;

/*{{save:dat_apps_total.csv}}*/
sel  '{siteid}'  as Site_ID
    ,cast(cast(count(distinct AppId) as format 'Z,ZZZ,ZZ9') as varchar(32)) as TotalApps
from dbql_core_hourly;

/*{{save:dat_daily_query_and_tactical_cnt.csv}}*/
select
 '{siteid}'  as Site_ID
,cast(LogTS as CHAR(10)) AS LogDate
,sum(Request_Cnt) AS RequestCnt
,sum(Query_Tactical_Cnt) AS TacticalQueryCnt
from dbql_core_hourly
group by 2
;



/*{{save:dat_statement_frequency.csv}}*/
select
     '{siteid}' as Site_ID
    ,StatementType as "Statement Type"
    ,cast(cast(sum(Request_Cnt) as format 'ZZZ,ZZZ,ZZ9') as varchar(32))  AS "Query Volume"
from dbql_core_hourly
group by 2
order by sum(Request_Cnt) desc
;



/*{{save:dat_apps_frequency.csv}}*/
select top 24
     '{siteid}' as Site_ID
    ,AppId
    ,cast(cast(sum(Request_Cnt) as format 'ZZZ,ZZZ,ZZ9') as varchar(32)) AS "Total Queries"
    ,cast(cast(sum(Returned_Row_Cnt) as bigint format 'ZZZ,ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) AS "Total Fetched Rows"
    ,cast(cast(sum(Returned_Row_Cnt)/sum(Request_Cnt) as integer format 'ZZZ,ZZZ,ZZ9') as varchar(32)) AS "Avg Rows Per Query"
from dbql_core_hourly
group by 2
order by sum(Returned_Row_Cnt)/sum(Request_Cnt) desc
;


/*{{save:dat_daily_data_transfer.csv}}*/
select '{siteid}'  as Site_ID
      ,LogDate
      ,SUM(HostRead_KB) as InboundKB
      ,SUM(HostWrite_KB) as OutboundKB
      ,CASE
          WHEN (InboundKB) > POWER(1024,4) THEN CAST((InboundKB) / POWER(1024, 4) AS DECIMAL(18,2)) || 'PB'
          WHEN (InboundKB) > POWER(1024,3) THEN CAST((InboundKB) / POWER(1024, 3) AS DECIMAL(18,2)) || 'TB'
          WHEN (InboundKB) > POWER(1024,2) THEN CAST((InboundKB) / POWER(1024, 2) AS DECIMAL(18,2)) || 'GB'
          WHEN (InboundKB) > POWER(1024,1) THEN CAST((InboundKB) / POWER(1024, 1) AS DECIMAL(18,2)) || 'MB'
          ELSE CAST((InboundKB) AS DECIMAL(18, 2)) || 'KB'
       END AS InboundAbbrev
      ,CASE
          WHEN (OutboundKB) > POWER(1024,4) THEN CAST((OutboundKB) / POWER(1024, 4) AS DECIMAL(18,2)) || 'PB'
          WHEN (OutboundKB) > POWER(1024,3) THEN CAST((OutboundKB) / POWER(1024, 3) AS DECIMAL(18,2)) || 'TB'
          WHEN (OutboundKB) > POWER(1024,2) THEN CAST((OutboundKB) / POWER(1024, 2) AS DECIMAL(18,2)) || 'GB'
          WHEN (OutboundKB) > POWER(1024,1) THEN CAST((OutboundKB) / POWER(1024, 1) AS DECIMAL(18,2)) || 'MB'
          ELSE CAST((OutboundKB) AS DECIMAL(18, 2)) || 'KB'
      END AS OutboundAbbrev
from cpu_summary_hourly
group by 2
order by 2
;
