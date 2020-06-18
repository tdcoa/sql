/*
##############################################
Query 20

Query Output File Name: DatabaseActivitySummary
Tableau Dashboard: Activity By Database

*/

/*{{save:DatabaseActivitySummary}}*/
  SELECT
		TMP.DatabaseName
	,	TMP.Tablename
	--,	TMP.LogDate
	, TMP.currentperm
	,SUM(CountOfUses) "Count Of Uses"
	,SUM(TotalCPU1) "Total Cpu"
	,SUM(IOInGBytes1)"Ioingbytes"

	FROM(
	SELECT
		o.QueryId QueryId
	,	o.LogDate AS LogDate
	,	o.objectdatabasename     as "DatabaseName"
	,	o.ObjectTableName        as "TableName"
	,t.currentperm as currentperm

	,	(o.freqofuse)         as "CountOfUses"
	,l.TotalCPU as TotalCPU
	,l.IOInGBytes AS IOInGBytes
	,	(t.currentperm*o.freqofuse/(sum(t.currentperm*o.freqofuse) over (partition by o.queryid))  )TableQueryPercent
	,	(l.IOInGBytes*TableQueryPercent ) as "IOInGBytes1"
	,	(l.TotalCpu*TableQueryPercent   ) as "TotalCPU1"

	FROM PDCRINFO.DBQLObjTbl_hst o

	LEFT JOIN
	(
	Select
   Tablename Tablename
   ,DatabaseName DatabaseName
   ,CURRENTPERM/1E9 AS currentperm
    FROM PDCRINFO.TableSpace_Hst a INNER JOIN Sys_Calendar.CALENDAR  c  ON a.Logdate = c.Calendar_date  WHERE  c.Calendar_date = a.Logdate
  AND a.Logdate = (select MAX(Logdate) from PDCRINFO.TableSpace_Hst )
  Group By 1,2,3

	)t
	on o.objectdatabasename = t.DatabaseName
	AND o.ObjectTableName = t.Tablename
	INNER JOIN
	(

		SELECT
			l.LogDate AS LogDate
			,l.queryid queryid

		,	COALESCE(spma.AvgIOPerReqGB, 1.0 / (1024 * 1024 * 1024))*l.TotalIOCount (FLOAT) AS IOInGBytes
		,	l.AmpCPUTime + l.ParserCPUTime         AS TotalCPU

		FROM PDCRINFO.DBQLogTbl_Hst  l
		LEFT JOIN
		(
			SELECT
		a.thedate
		,((SUM((a.LogicalDeviceReadKB + a.LogicalDeviceWriteKB) / (1024 * 1024) ) (float))
					/ SUM(a.FileAcqs +a.FilePreReads +a.MemTextPageReads +a.MemCtxtPageWrites+a.MemCtxtPageReads+a.FileWrites ) )(decimal(18,10)) AvgIOPerReqGB
		FROM
		(
		SELECT
		thedate thedate
		,FileAcqs FileAcqs
		,FilePreReads FilePreReads
		,MemTextPageReads MemTextPageReads
		,MemCtxtPageWrites MemCtxtPageWrites
		,MemCtxtPageReads MemCtxtPageReads
		,FileWrites FileWrites
		,vproc1
		,( FileAcqReadKB + FilePreReadKB +
		  /* paging or swapping count times pagesize (= 4K) */
		  (MemTextPageReads + MemCtxtPageReads ) * 4 ) AS LogicalDeviceReadKB
		,( FileWriteKB +
		  /* paging or swapping count times pagesize (= 4K) */
		  MemCtxtPageWrites * 4 ) AS LogicalDeviceWriteKB
		  FROM PDCRINFO.ResUsageSPMA_Hst
		  WHERE  thedate BETWEEN date -1 and date -1
		  GROUP BY 1,2,3,4,5,6,7,8,9,10
		  )a
		  Where   vproc1 > 0
		  Group BY 1
		) spma
		ON l.LogDate = spma.thedate

		Where l.LogDate BETWEEN date -1 and date -1


		Group By 1,2,3,4
	)l
	ON o.queryid = l.queryid
	AND o.logdate = l.logdate
WHERE o.LogDate BETWEEN date -1 and date -1

Group By 1,2,3,4,5,6,7,8
)TMP
Group By 1,2,3;
