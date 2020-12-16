/* Start COA: Workload Analytics
   see comments about each SQL step inline below.

Parameters:
  - startdate:    {startdate}
  - enddate:      {enddate}
  - siteid:       {siteid}
  - resusagespma: {resusagespma}
*/


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
