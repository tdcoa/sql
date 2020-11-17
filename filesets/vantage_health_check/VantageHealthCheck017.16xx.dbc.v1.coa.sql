/*
##############################################3
Query 17

Query Output File Name: TopDatabasebySpace
Tableau Dashboard: Database Size, Top Database Size

*/
 
/*{{save:TopDatabasebySpace.csv}}*/
Select
	Rank()  OVER (Order by TMP.CURRENTPERM DESC ) as CURRENTPERMRnk
	,TMP.LogDate AS LogDate
	,TMP.DatabaseName AS DatabaseName
	,TMP.CURRENTPERM AS "DB Size GB"
	,TMP.PEAKPERM AS "PEAK DB Size GB"
	,TMP.MAXPERM AS "MAX DB Size GB"
	,TMP.CurrentPermSkew AS CURRENTPERMSKEW
	,TMP.PermPctUsed AS PERMPCTUSED
	FROM
	(SELECT
	    Case when extract(hour from current_time) LT 6 THEN current_date-1
	     ELSE date END as LogDate
	     ,DatabaseName as DatabaseName
	     ,AccountName as AccountName
	     ,SUM(CURRENTPERM)/1E9  as CURRENTPERM
	     ,SUM(PEAKPERM)/1E9 as PEAKPERM
	     ,SUM(MAXPERM)/1E9 as MAXPERM
	     ,100 * (1-(AVG(a.CURRENTPERM)/NULLIFZERO(MAX(a.CURRENTPERM))))     AS CurrentPermSkew
	     ,(SUM(CURRENTPERM)/NULLIFZERO(SUM(MAXPERM ))) * 100                AS PermPctUsed
	     FROM DBC.DISKSPACE  a
	     WHERE   a.maxPERM > 0
	Group BY 1,2,3) TMP
;
