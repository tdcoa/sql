
LOCKING ROW FOR ACCESS 
SEL	'1) 100K - 500 K' AS CumulativeCPU_Thresholds, SUM(
CASE	
	WHEN CumulativeCPU_Suspect >= 100000 
	AND CumulativeCPU_Suspect < 500000 THEN 1 
END) AS UniqueSuspectSQLCountLT25K, SUM(
CASE	
	WHEN CumulativeCPU_Overall >= 100000 
	AND CumulativeCPU_Overall < 500000 THEN 1 
END) OverallUniqueSQLCount 
FROM(
SEL	SQLRowHash, SUM(CumulativeCPU_Overall) AS CumulativeCPU_Overall,
		SUM(CumulativeCPU_Suspect) AS CumulativeCPU_Suspect 
FROM(
SEL	SQLRowHash, SUM(CumulativeCPU) AS CumulativeCPU_Overall, SUM(
CASE	
	WHEN SuspectCount = 1 THEN CumulativeCPU 
END) AS CumulativeCPU_Suspect 
FROM(
SELECT	 HASHROW(COALESCE(sq.SQLTextInfo, md.querytext)) SQLRowHash,
		(AMPCPUTime)CumulativeCPU, CAST((
CASE	
	WHEN ampcputime > 0 
	AND ampcputime < 25000 
	AND(((((AMPCPUTime + Parsercputime) * 1000) / NULLIFZERO(TotalIOCount)) > 3) 
	OR((TotalIOCount / NULLIFZERO((AMPCPUTime + ParserCPUTime) * 1000)) > 3)
	OR((100 - (NULLIFZERO(AMPCPUTime / NULLIFZERO(NumOfActiveAMPs)) / NULLIFZERO(MaxAMPCPUTime) * 100)) > 50) 
	OR((100 - (NULLIFZERO(TotalIOCount / NULLIFZERO(NumOfActiveAMPs)) / NULLIFZERO(maxampio)) * 100) > 50)) THEN 1 
	ELSE 0 
END) AS INTEGER) SuspectCount 
FROM	   pdcrinfo.DBQLogTbl_hst md  LEFT OUTER JOIN pdcrinfo.DBQLSQLTbl_hst sq  
	ON  md.queryid = sq.queryid 
	AND md.logdate = sq.logdate  
WHERE	  md.LogDate BETWEEN DATE - 30 
	AND DATE - 1 
	AND md.numsteps > 0  
	AND md.ampcputime > 0 
	AND sq.sqlrowno = 1) a 
GROUP BY SQLRowHash, SuspectCount) temp 
GROUP BY 1)x 
GROUP BY 1 
HAVING	UniqueSuspectSQLCountLT25K IS NOT NULL 
	OR OverallUniqueSQLCount IS NOT NULL 
UNION	ALL 
SEL	'2) 500K - 1Mn' AS CumulativeCPU_Thresholds, SUM(
CASE	
	WHEN CumulativeCPU_Suspect >= 500000 
	AND CumulativeCPU_Suspect < 1000000 THEN 1 
END) AS UniqueSuspectSQLCountLT25K, SUM(
CASE	
	WHEN CumulativeCPU_Overall >= 500000 
	AND CumulativeCPU_Overall < 1000000 THEN 1 
END) OverallUniqueSQLCount 
FROM(
SEL	SQLRowHash, SUM(CumulativeCPU_Overall) AS CumulativeCPU_Overall,
		SUM(CumulativeCPU_Suspect) AS CumulativeCPU_Suspect 
FROM(
SEL	SQLRowHash, SUM(CumulativeCPU) AS CumulativeCPU_Overall, SUM(
CASE	
	WHEN SuspectCount = 1 THEN CumulativeCPU 
END) AS CumulativeCPU_Suspect 
FROM(
SELECT	 HASHROW(COALESCE(sq.SQLTextInfo, md.querytext)) SQLRowHash,
		(AMPCPUTime)CumulativeCPU, CAST((
CASE	
	WHEN ampcputime > 0 
	AND ampcputime < 25000 
	AND(((((AMPCPUTime + Parsercputime) * 1000) / NULLIFZERO(TotalIOCount)) > 3)
	OR((TotalIOCount / NULLIFZERO((AMPCPUTime + ParserCPUTime) * 1000)) > 3) 
	OR((100 - (NULLIFZERO(AMPCPUTime / NULLIFZERO(NumOfActiveAMPs)) / NULLIFZERO(MaxAMPCPUTime) * 100)) > 50)
	OR((100 - (NULLIFZERO(TotalIOCount / NULLIFZERO(NumOfActiveAMPs)) / NULLIFZERO(maxampio)) * 100) > 50))THEN 1 
	ELSE 0 
END) AS INTEGER) SuspectCount 
FROM	   pdcrinfo.DBQLogTbl_hst md LEFT OUTER JOIN pdcrinfo.DBQLSQLTbl_hst sq 
	ON  md.queryid = sq.queryid 
	AND md.logdate = sq.logdate  
WHERE	  md.LogDate BETWEEN DATE - 30 
	AND DATE - 1 
	AND md.numsteps > 0  
	AND md.ampcputime > 0 
	AND sq.sqlrowno = 1) a 
GROUP BY SQLRowHash, SuspectCount) temp 
GROUP BY 1)x 
GROUP BY 1 
HAVING	UniqueSuspectSQLCountLT25K IS NOT NULL 
	OR OverallUniqueSQLCount IS NOT NULL 
UNION	ALL 
SEL	'3) GT 1Mn' AS CumulativeCPU_Thresholds, SUM(
CASE	
	WHEN CumulativeCPU_Suspect >= 1000000 THEN 1 
END) AS UniqueSuspectSQLCountLT25K, SUM(
CASE	
	WHEN CumulativeCPU_Overall >= 1000000 THEN 1 
END) OverallUniqueSQLCount 
FROM	(
SEL	SQLRowHash, SUM(CumulativeCPU_Overall) AS CumulativeCPU_Overall,
		SUM(CumulativeCPU_Suspect) AS CumulativeCPU_Suspect 
FROM	(
SEL	SQLRowHash, SUM(CumulativeCPU) AS CumulativeCPU_Overall, SUM(
CASE	
	WHEN SuspectCount = 1 THEN CumulativeCPU 
END) AS CumulativeCPU_Suspect 
FROM	(
SELECT	 HASHROW(COALESCE(sq.SQLTextInfo, md.querytext)) SQLRowHash,
		(AMPCPUTime)CumulativeCPU, CAST((
CASE	
	WHEN ampcputime > 0 
	AND ampcputime < 25000 
	AND(((((AMPCPUTime + Parsercputime) * 1000) / NULLIFZERO(TotalIOCount)) > 3) 
	OR((TotalIOCount / NULLIFZERO((AMPCPUTime + ParserCPUTime) * 1000)) > 3)
	OR((100 - (NULLIFZERO(AMPCPUTime / NULLIFZERO(NumOfActiveAMPs)) / NULLIFZERO(MaxAMPCPUTime) * 100)) > 50)
	OR((100 - (NULLIFZERO(TotalIOCount / NULLIFZERO(NumOfActiveAMPs)) / NULLIFZERO(maxampio)) * 100) > 50))THEN 1 
	ELSE 0 
END) AS INTEGER) SuspectCount 
FROM	   pdcrinfo.DBQLogTbl_hst md LEFT OUTER JOIN pdcrinfo.DBQLSQLTbl_hst sq  
	ON  md.queryid = sq.queryid 
	AND md.logdate = sq.logdate  
WHERE	  md.LogDate BETWEEN DATE - 30 
	AND DATE - 1 
	AND md.numsteps > 0  
	AND md.ampcputime > 0 
	AND sq.sqlrowno = 1) a 
GROUP BY SQLRowHash, SuspectCount)temp 
GROUP BY 1)x 
GROUP BY 1 
ORDER BY 1 
HAVING	UniqueSuspectSQLCountLT25K IS NOT NULL 
	OR OverallUniqueSQLCount IS NOT NULL
