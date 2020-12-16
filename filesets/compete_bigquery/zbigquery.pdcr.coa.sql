/* Start COA: Workload Analytics
   see comments about each SQL step inline below.

Parameters:
  - startdate:    {startdate}
  - enddate:      {enddate}
  - siteid:       {siteid}
  - resusagespma: {resusagespma}
*/

/* DBQL_CORE pulls...  */

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



/*{{save:dat_disk_space_total.csv}}*/
select
  '{siteid}'  as Site_ID
 ,cast(cast(sum(MaxPermGB) / 1024 as format 'Z,ZZZ,ZZ9.999') as varchar(32)) as TotalMaxPermTB
 ,cast(cast(sum(CurrentPermGB) / 1024 as format 'Z,ZZZ,ZZ9.999') as varchar(32)) as TotalCurrPermTB
from db_objects_cds
where DBName = '*** Total ***'
  and LogDate = (sel max(LogDate) from db_objects_cds)
;


/* slide 5 */
/*{{save:dat_join_frequency.csv}}*/
 SELECT
   '{siteid}' as Site_ID
  ,CASE WHEN QueryObjCount <= 5 THEN (QueryObjCount (FORMAT 'Z9') (CHAR(2))) ELSE ' 5+' END as "Number of Joins"
  ,cast(cast(count(*) as BigInt format 'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as "Number of Queries"
FROM
(
SELECT DBQL_Obj.QueryID
      ,count(*) as QueryObjCount
  FROM PDCRInfo.DBQLObjTbl_Hst DBQL_Obj
 WHERE DBQL_Obj.ObjectType in ('Tab', 'Viw')
   AND DBQL_Obj.LogDate BETWEEN {startdate} AND {enddate}
 GROUP BY 1
) dt1
GROUP BY 2
ORDER BY 2;




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


/* slide 7 */
/*{{save:dat_tables_size10g_cnt.csv}}*/
SELECT
    '{siteid}' as Site_ID
    ,cast(cast(coalesce(count(*), 0) as format 'ZZZ,ZZZ,ZZ9') as varchar(32))  AS Tbl10gCnt
from tables_size_10g;


/* slide 7 */
/*{{save:dat_tables_size10g_list.csv}}*/
SELECT
    '{siteid}' as Site_ID
   ,DataBaseName
   ,TableName
   ,cast(cast(CurrentPerm_GB as format 'ZZZ,ZZZ,ZZ9.9') as varchar(32)) AS "CurrentPerm GB"
FROM tables_size_10g
ORDER BY CurrentPerm_GB DESC;


/* slides 8 and 9 */

create volatile table tables_insupddel as
(
SELECT QryLog.LogDate
      ,QryObj.ObjectDatabaseName
      ,QryObj.ObjectTableName
      ,COUNT(*) StatementCountPerTable
 FROM PDCRINFO.DBQLogTbl_Hst QryLog
INNER JOIN PDCRINFO.DBQLObjTbl_Hst QryObj
   ON QryLog.LogDate = QryObj.LogDate
  AND QryLog.QueryID = QryObj.QueryID
WHERE
      QryLog.LogDate BETWEEN {startdate} AND {enddate}
  AND QryLog.StatementType IN ('insert', 'update', 'delete')
  AND QryObj.ObjectType = 'Tab'
GROUP BY 1,2,3
HAVING StatementCountPerTable > 1500
) with data
no primary index
on commit preserve rows
;


/* slide 8 */
/* subtitle */
/*{{save:dat_avg_1500_tblcnt.csv}}*/
sel '{siteid}' as Site_ID
   ,count(*) / count(distinct LogDate) as AvgTblCnt
from tables_insupddel;

/* graph data */
/*{{save:dat_tables_1500_insupdel.csv}}*/
sel '{siteid}' as Site_ID
   ,LogDate
   ,count(*) as TableCnt
from tables_insupddel
group by 2
order by 2;


/* slide 9 */
/*{{save:dat_dbs_1500_insupdel.csv}}*/
sel '{siteid}' as Site_ID
   ,ObjectDatabaseName
   ,cast(cast(avg(StatementCountPerTable) as integer format 'ZZZ,ZZZ,ZZ9') as varchar(32)) as "Avg Statement Count Per Table"
from tables_insupddel
group by 2
order by avg(StatementCountPerTable) desc;

/*{{save:account.csv}}*/
Select
 trim('{account}') as AccountName
,trim('{siteid}') as Site_ID
,trim('{your_name}') as Your_Name
,trim('{your_title}') as Your_Title
,cast(Current_Date as DATE format 'mmmm,byyyy')(varchar(32)) as Month_Year
;

/*{{pptx:big_query_migration_blockers.pptx}}*/

/* End COA: WL Analytics */
