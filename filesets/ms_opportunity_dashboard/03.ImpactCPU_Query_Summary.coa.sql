SELECT	Thresholds ,COUNT(1)  Unique_SQL_Count  
FROM(
SELECT	SUBSTR(sq.SQLTextInfo, 1, 200) PartOfQueryText, 
CASE	
	WHEN MaxAMPCPUTime * NumOfActiveAMPs BETWEEN 100000 AND 500000 THEN '100K - 500 K'  
	WHEN MaxAMPCPUTime * NumOfActiveAMPs BETWEEN 500000.01 
	AND 1000000 THEN '500K - 1Mn'  
	WHEN MaxAMPCPUTime * NumOfActiveAMPs > 1000000 THEN 'GT 1Mn' 
END	Thresholds, COUNT(1) QueryCount  
FROM	pdcrinfo.DBQLogTbl_hst lg, pdcrinfo.DBQLSQLTbl_hst sq 
WHERE	 lg.procid = sq.procid 
	AND  lg.queryid = sq.queryid  
	AND lg.logdate = sq.logdate 
	AND lg.LogDate BETWEEN  DATE - 30 
	AND DATE - 1 
	AND lg.numsteps > 0 
	AND MaxAMPCPUTime * NumOfActiveAMPs >= 100000  
	AND sq.sqlrowno = 1 
GROUP BY 1, 2)a 
GROUP BY 1 
ORDER BY 1;