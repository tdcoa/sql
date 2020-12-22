LOCK	ROW FOR ACCESS 
SELECT	 CPU_Thresholds, COUNT(1) Unique_SQL_Count ,
SUM(
CASE	
	WHEN CPUSkew_GT50PCT > 0 THEN 1 
	ELSE 0 
END) CPU_Skew_GT_50_PCT  ,
SUM(
CASE	
	WHEN IOSkew_GT50PCT > 0 THEN 1 
	ELSE 0 
END) IO_Skew_GT_50_PCT, 
SUM(
CASE	
	WHEN PJI_GT3 > 0 THEN 1 
	ELSE 0 
END) PJI_GT_3  ,
SUM(
CASE	
	WHEN PJI_GT5 > 0 THEN 1 
	ELSE 0 
END) PJI_GT_5, 
SUM(
CASE	
	WHEN PJI_GT10 > 0 THEN 1 
	ELSE 0 
END) PJI_GT_10  ,
SUM(
CASE	
	WHEN UII_GT3 > 0 THEN 1 
	ELSE 0 
END) UII_GT_3,
SUM(
CASE	
	WHEN UII_GT5 > 0 THEN 1 
	ELSE 0 
END) UII_GT_5,
SUM(
CASE	
	WHEN UII_GT10 > 0 THEN 1 
	ELSE 0 
END) UII_GT_10  , 
SUM(
CASE	
	WHEN PJI_GT3 > 0  
	OR UII_GT3 > 0  
	OR CPUSkew_GT50PCT > 0  
	OR IOSkew_GT50PCT > 0 THEN 1 
	ELSE 0  
END) OverallSuspect3Count  ,
SUM(
CASE	
	WHEN PJI_GT5 > 0  
	OR UII_GT5 > 0  
	OR CPUSkew_GT50PCT > 0  
	OR IOSkew_GT50PCT > 0 THEN 1 
	ELSE 0  
END) OverallSuspect5Count  , SUM(
CASE	
	WHEN PJI_GT10 > 0  
	OR UII_GT10 > 0  
	OR CPUSkew_GT50PCT > 0  
	OR IOSkew_GT50PCT > 0 THEN 1 
	ELSE 0  
END) OverallSuspect10Count   
FROM(
SELECT	   HASHROW(COALESCE(sq.SQLTextInfo, md.querytext)) SQLRowHash,
		CASE 
	WHEN AMPCPUTime >= 0 
	AND AMPCPUTime < 10000 THEN '00) 0-10K'   
	WHEN AMPCPUTime >= 10000 
	AND AMPCPUTime < 25000 THEN '00) 10-25K'   
	WHEN AMPCPUTime >= 25000 
	AND AMPCPUTime < 125000 THEN '1) 25K - 125 K'  
	WHEN AMPCPUTime BETWEEN 125000 AND 250000 THEN '2) 125K - 250 K'   
	WHEN AMPCPUTime > 250000 THEN '3) GT 250K'   
END	CPU_Thresholds  , SUM(
CASE	
	WHEN(((AMPCPUTime + Parsercputime) * 1000) / NULLIFZERO(TotalIOCount)) > 3 THEN 1  
	ELSE 0 
END) PJI_GT3,   SUM(
CASE	
	WHEN(TotalIOCount / NULLIFZERO((AMPCPUTime + ParserCPUTime) * 1000)) > 3 THEN 1  
	ELSE 0 
END) UII_GT3   , SUM(
CASE	
	WHEN(((AMPCPUTime + Parsercputime) * 1000) / NULLIFZERO(TotalIOCount)) > 5 THEN 1  
	ELSE 0 
END) PJI_GT5   , SUM(
CASE	
	WHEN(TotalIOCount / NULLIFZERO((AMPCPUTime + ParserCPUTime) * 1000)) > 5 THEN 1  
	ELSE 0 
END) UII_GT5   , SUM(
CASE	
	WHEN(((AMPCPUTime + Parsercputime) * 1000) / NULLIFZERO(TotalIOCount)) > 10 THEN 1  
	ELSE 0 
END) PJI_GT10   , SUM(
CASE	
	WHEN(TotalIOCount / NULLIFZERO((AMPCPUTime + ParserCPUTime) * 1000)) > 10 THEN 1  
	ELSE 0 
END) UII_GT10   , SUM(
CASE	
	WHEN(100 - (NULLIFZERO(AMPCPUTime / NULLIFZERO(NumOfActiveAMPs)) / NULLIFZERO(MaxAMPCPUTime) * 100)) > 50 THEN 1    
	ELSE 0 
end) CPUSkew_GT50PCT, SUM(
CASE	
	WHEN(100 - (NULLIFZERO(TotalIOCount / NULLIFZERO(NumOfActiveAMPs)) / NULLIFZERO(maxampio)) * 100) > 50 THEN 1    
	ELSE 0 
END) AS IOSkew_GT50PCT   
FROM	   pdcrinfo.DBQLogTbl_hst md  LEFT OUTER JOIN pdcrinfo.DBQLSQLTbl_hst sq   
	ON  md.queryid = sq.queryid 
	AND md.logdate = sq.logdate    
WHERE	  md.LogDate BETWEEN DATE - 30 
	AND DATE - 1 
	AND md.numsteps > 0   
	AND md.ampcputime >= 25000  
	AND sq.sqlrowno = 1  
	AND CPU_Thresholds IS NOT NULL  
GROUP BY 1, 2)a  
GROUP BY 1  
ORDER BY 1;
