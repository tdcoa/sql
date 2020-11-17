/*
##############################################
Query 20

Query Output File Name: DatabaseActivitySummary
Tableau Dashboard: Activity By Database

*/ 

/*{{save:DatabaseActivitySummary.csv}}*/
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
	,	cast(o.CollectTimeStamp as date) AS LogDate
	,	o.objectdatabasename     as "DatabaseName"
	,	o.ObjectTableName        as "TableName"
	,t.currentperm as currentperm

	,	(o.freqofuse)         as "CountOfUses"
	,l.TotalCPU as TotalCPU
	,l.IOInGBytes AS IOInGBytes
	,	(t.currentperm*o.freqofuse/(sum(t.currentperm*o.freqofuse) over (partition by o.queryid))  )TableQueryPercent
	,	(l.IOInGBytes*TableQueryPercent ) as "IOInGBytes1"
	,	(l.TotalCpu*TableQueryPercent   ) as "TotalCPU1"

	FROM DBC.DBQLObjTbl o

	LEFT JOIN
	(
	Select
   Tablename Tablename
   ,DatabaseName DatabaseName
   ,CURRENTPERM/1E9 AS currentperm
    FROM dbc.TableSize
  Group By 1,2,3

	)t
	on o.objectdatabasename = t.DatabaseName
	AND o.ObjectTableName = t.Tablename
	INNER JOIN
	(

		SELECT
			cast(l.StartTime as date) AS LogDate
			,l.queryid queryid

		,	COALESCE(spma.AvgIOPerReqGB, 1.0 / (1024 * 1024 * 1024))*l.TotalIOCount (FLOAT) AS IOInGBytes
		,	l.AmpCPUTime + l.ParserCPUTime         AS TotalCPU

		FROM DBC.DBQLogTbl  l
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
		  FROM dbc.ResUsageSPMA
		  WHERE  thedate BETWEEN date -1 and date -1
		  GROUP BY 1,2,3,4,5,6,7,8,9,10
		  )a
		  Where   vproc1 > 0
		  Group BY 1
		) spma
		ON cast(l.StartTime as date) = spma.thedate

		Where cast(l.StartTime as date) BETWEEN date -1 and date -1


		Group By 1,2,3,4
	)l
	ON o.queryid = l.queryid
--	AND cast(o.CollectTimeStamp as date) = l.logdate
WHERE cast(o.CollectTimeStamp as date) BETWEEN date -1 and date -1

Group By 1,2,3,4,5,6,7,8
)TMP
Group By 1,2,3;
