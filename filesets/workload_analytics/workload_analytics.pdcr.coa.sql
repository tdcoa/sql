/* Start COA: Workload Analytics
   see comments about each SQL step inline below.

Parameters:
  - startdate:    {startdate}
  - enddate:      {enddate}
  - siteid:       {siteid}
  - resusagespma: {resusagespma}
*/

/* This is similar to dim_users/top users but logic to extract not same, this filters more*/

/* Slide 2 - active users, total users */
SELECT  
'System' as SystemName
,dbs.DatabaseName as UserName
,actusr.ActiveUserName
,actusr.ActiveStartDate
,actusr.ActiveEndDate
from dbc.databasesV dbs
left join
(
  Select 
   UserName as ActiveUserName
  ,min(cast(StartTime as date)) as ActiveStartDate
  ,max(cast(StartTime as date)) as ActiveEndDate
  from pdcrinfo.dbqlogtbl_hst
  where logdate  BETWEEN current_date - 90   AND current_date
  group by 1
) actusr
on actusr.ActiveUserName = dbs.DatabaseName 
where dbkind = 'U'
  and OwnerName <> 'DBC'
  and DatabaseName not in ('EXTUSER','Viewpoint')
;


/*graph on slide 3 */
/*{{save:dat_daily_data_transfer.csv}}*/
select LogDate
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
group by 1
order by 1
;


/*  titles on slide 4 */
/*{{save:dat_avg_query_and_tactical_pct.csv}}*/
select 
 '{siteid}'  as Site_ID
,avg(RequestCnt) AS AvgQueriesPerDay
,AvgQueriesPerDate / (24*3600) AS AvgQueriesPerSecond
,sum(TacticalQueryCnt) / sum(RequestCnt) * 100 AS TacticalPct
FROM
(
 cast(LogTS as CHAR(10)) AS LogDate
,sum(Request_Cnt) AS RequestCnt
,sum(Query_Tactical_Cnt) AS TacticalQueryCnt
from dbql_core_hourly
) dt1
;


/* graph on slide 4 */
/*{{save:dat_daily_query_and_tactical_cnt.csv}}*/
select 
 '{siteid}'  as Site_ID
,cast(LogTS as CHAR(10)) AS LogDate
,sum(Request_Cnt) AS RequestCnt
,sum(Query_Tactical_Cnt) AS TacticalQueryCnt
from dbql_core_hourly
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
   AND DBQL_Qry.LogDate BETWEEN {startdate} AND {enddate}
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
select 
     '{siteid}' as Site_ID
    ,AppId
    ,sum(Request_Cnt) AS RequestCnt
    ,sum(Returned_Row_Cnt) AS TotalFetchedRows
    ,TotalFetchedRows/RequestCnt AS AvgRowsPerQry
from dbql_core_hourly
group by 2
order by 4 desc;


/* End COA: WL Analytics */
