/* CDS history sql

   parameters:
   - tablespace_hst:  {tablespace_hst}
   - startdate:       {startdate}
   - enddate:         {enddate}

*/

/*{{save:cds_history.csv}}*/
SELECT
 h.LogDate
,trim(h.DatabaseName) as "Database"
,trim(h.TableName) as "Table"
,cast(SUM(h.CurrentPerm)/1e9 as decimal(18,3)) AS "Current PermGB"
,cast(avg(d.PermSpace)/1e9 as decimal(18,3)) as "Max PermGB"
,cast("Current PermGB"/"Max PermGB" as decimal(4,3)) as CDS_Pct
,cast(cast(CDS_Pct*100 as decimal(4,1) format '99.9') as char(4))||'%' as "CDS%"
,rank() over(partition by "Database" order by "Current PermGB" desc) as RankGB
FROM {tablespace_hst} /* pdcrinfo.TableSpace_Hst */ as h
JOIN dbc.databasesV as d
  on d.DatabaseName = h.DatabaseName
WHERE h.LogDate BETWEEN {startdate} and {enddate}
GROUP BY LogDate,"Database",rollup("Table")
;
