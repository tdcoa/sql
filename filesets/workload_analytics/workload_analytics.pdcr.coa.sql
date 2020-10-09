/* Start COA: Workload Analytics
   see comments about each SQL step inline below.

Parameters:
  - startdate:    {startdate}
  - enddate:      {enddate}
  - siteid:       {siteid}
  - resusagespma: {resusagespma}
*/

/* Slide 2 - active users, from fs top_users */

/* Slide 2 - object counts, from db_objects - use dat_objectkind_count-total.csv */

/* Slide 2 - query counts. Slide 4 - titles */
/*{{save:dat_query_counts.csv}}*/
select 
 '{siteid}'  as Site_ID
,count(distinct cast(LogTS as char(10))) as DayCnt
,sum(Request_Cnt) AS TotalRequestCnt
,TotalRequestCnt / DayCnt AS AvgQryPerDay
,TotalRequestCnt / DayCnt  / 1e6 AS AvgMilQryPerDay
,AvgMilQryPerDay * 30 AS AvgMilQryPerMonth
,TotalRequestCnt / DayCnt / 3600 AS AvgQryPerSecond
,TotalRequestCnt * 365 / DayCnt AS QryPerYear
,TotalRequestCnt * 365 / DayCnt / 1e9 AS BilQryPerYear
,sum(Query_Tactical_Cnt) AS TotalTacticalCnt
,TotalTacticalCnt / TotalRequestCnt * 100 AS TacticalPct
from dbql_core_hourly
;


/* Slide 2 */
/*{{save:dat_apps_total.csv}}*/
sel  '{siteid}'  as Site_ID, count(distinct AppId) as TotalApps
from dbql_core_hourly;


/*Slide 3  - graph */
/*{{save:dat_daily_data_transfer.csv}}*/
select '{siteid}'  as Site_ID
      ,LogDate
      ,SUM(HostRead_KB) as InboundKB
      ,SUM(HostWrite_KB) as OutboundKB
      ,CASE
          WHEN (InboundKB) > POWER(1024,4) THEN CAST((InboundKB) / POWER(1024, 4) AS DECIMAL(5,2)) || 'PB'
          WHEN (InboundKB) > POWER(1024,3) THEN CAST((InboundKB) / POWER(1024, 3) AS DECIMAL(5,2)) || 'TB'
          WHEN (InboundKB) > POWER(1024,2) THEN CAST((InboundKB) / POWER(1024, 2) AS DECIMAL(5,2)) || 'GB'
          WHEN (InboundKB) > POWER(1024,1) THEN CAST((InboundKB) / POWER(1024, 1) AS DECIMAL(5,2)) || 'MB'
          ELSE CAST((InboundKB) AS DECIMAL(5, 2)) || 'KB'
       END AS InboundAbbrev
      ,CASE
          WHEN (OutboundKB) > POWER(1024,4) THEN CAST((OutboundKB) / POWER(1024, 4) AS DECIMAL(5,2)) || 'PB'
          WHEN (OutboundKB) > POWER(1024,3) THEN CAST((OutboundKB) / POWER(1024, 3) AS DECIMAL(5,2)) || 'TB'
          WHEN (OutboundKB) > POWER(1024,2) THEN CAST((OutboundKB) / POWER(1024, 2) AS DECIMAL(5,2)) || 'GB'
          WHEN (OutboundKB) > POWER(1024,1) THEN CAST((OutboundKB) / POWER(1024, 1) AS DECIMAL(5,2)) || 'MB'
          ELSE CAST((OutboundKB) AS DECIMAL(5, 2)) || 'KB'
      END AS OutboundAbbrev
from cpu_summary_hourly
group by 2
order by 2
;



/* graph on slide 4 */
/*{{save:dat_daily_query_and_tactical_cnt.csv}}*/
select 
 '{siteid}'  as Site_ID
,cast(LogTS as CHAR(10)) AS LogDate
,sum(Request_Cnt) AS RequestCnt
,sum(Query_Tactical_Cnt) AS TacticalQueryCnt
from dbql_core_hourly
group by 2                    
;


/* slide 5 */
/*{{save:dat_join_frequency.csv}}*/
 SELECT
   '{siteid}' as Site_ID
  ,CASE WHEN QueryObjCount <= 5 THEN (QueryObjCount (FORMAT 'Z9') (CHAR(2))) ELSE ' 5+' END JoinObjs
  ,count(*) as RequestCnt
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


/* slide 5 */
/*{{save:dat_statement_frequency.csv}}*/
select 
     '{siteid}' as Site_ID
    ,StatementType
    ,sum(Request_Cnt) AS RequestCnt
from dbql_core_hourly
group by 2
order by 3 desc;


/* slide 6 */
/*{{save:dat_apps_frequency.csv}}*/
select top 24
     '{siteid}' as Site_ID
    ,AppId
    ,sum(Request_Cnt) AS RequestCnt
    ,sum(Returned_Row_Cnt) AS TotalFetchedRows
    ,TotalFetchedRows/RequestCnt AS AvgRowsPerQry
from dbql_core_hourly
group by 2
order by 4 desc;


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
    ,coalesce(count(*), 0) AS Tbl10gCnt
from tables_size_10g;


/* slide 7 */
/*{{save:dat_tables_size10g_list.csv}}*/
SELECT	
    '{siteid}' as Site_ID
   ,DataBaseName
   ,TableName
   ,CurrentPerm_GB
FROM tables_size_10g
ORDER BY 4 DESC;


/* slides 8 and 9 */

create volatile table tables_insupddel as
(
SELECT QryLog.LogDate
      ,QryObj.ObjectDatabaseName
      ,QryObj.ObjectTableName
      ,COUNT(QryLog.StatementType) StatementCountPerTable
 FROM PDCRINFO.DBQLogTbl_Hst QryLog
INNER JOIN PDCRINFO.DBQLObjTbl_Hst QryObj
   ON QryLog.LogDate = QryObj.LogDate
  AND QryLog.QueryID = QryObj.QueryID
WHERE
      QryLog.LogDate BETWEEN {startdate} AND {enddate}
  AND QryLog.StatementType IN ('insert', 'update', 'delete')
  AND QryLog.ObjectType = 'Tab'
GROUP BY 1,2,3
HAVING StatementCountPerTable > 1500
) with data 
no primary index 
on commit preserve rows
;


/* slide 8 */
/* subtitle */
/*{{save:dat_avg_1500_tblcnt.csv}}*/
sel '{siteid}' as Site_ID, count(*) / count(distinct LogDate) as AvgTblCnt;

/* graph data */
/*{{save:dat_tables_1500_insupdel.csv}}*/
sel '{siteid}' as Site_ID
   ,LogDate
   ,count(*) as TableCnt
from table tables_insupddel
group by 2
order by 2;   


/* slide 9 */
/*{{save:dat_dbs_1500_insupdel.csv}}*/
sel '{siteid}' as Site_ID
   ,ObjectDatabaseName
   ,avg(StatementCountPerTable) as AvgStCntPerTbl
from table tables_insupddel
group by 2
order by 3 desc;


/*{{pptx:big_query_migration_blockers.pptx}}*/                                                                   
/*{{pptx:workload_characteristics.pptx}}*/

/* End COA: WL Analytics */
