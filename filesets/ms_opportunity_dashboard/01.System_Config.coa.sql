SELECT	 LogDate, NodeType, NodeCPUs, Nodes, (Nodes * NodeCPUs * 3600) AS CPUSec_Hr,
CPUSec_Hr * 24 AS System_CPU,  
CASE	
	WHEN NodeType LIKE '%No_AMP%' THEN 0 
	ELSE System_CPU * .8 
END	AS User_CPU, Gateways, Gateways, MemSize_GBs,  AMPSPACE_GBs,
		(AMPs * AMPSPACE_GBs) / 1024 AS SystemSpace_TBs, NodeAMPs, NodePEs,
		AMPs, PEs, (
SEL	AVG(TotalCPU)  
FROM(
SELECT	logdate, SUM(AMPCPUTime + ParserCPUTime) AS TotalCPU 
FROM	pdcrinfo.dbqlogtbl_hst 
WHERE	logdate  BETWEEN DATE - 31 
	AND DATE - 1 
GROUP BY 1) a) AS Total_Monthly_AVG_CPU, (
SELECT	SUM(AMPCPUTime + ParserCPUTime) AS TotalCPU  
FROM	pdcrinfo.dbqlogtbl_hst 
WHERE	logdate BETWEEN DATE - 31 
	AND DATE - 1 ) AS Total_Monthly_CPU 
FROM(
SELECT	LogDate,  NodeType, NCPU AS NodeCPUs, COUNT(DISTINCT(dt.NodeID)) AS Nodes,
		MAX(dt.AMPs) * Nodes AS AMPs, MAX(dt.PEs) * Nodes AS PEs,  MAX(dt.GTW) * Nodes AS Gateways,
		MAX(MemSize) / 1024 AS MemSize_GBs, MAX(AMPSize) / 1073741824 AS AMPSPACE_GBs,
		 MAX(dt.AMPs) AS NodeAMPs, MAX(dt.PEs) AS NodePEs 
FROM(
SELECT	PMA.LogDate, NodeID, 
CASE	
	WHEN AMPS > 0 THEN Model  
	ELSE Model || 'No_AMP' 
END	AS NodeType, NCPU, AMPs, PEs, GTW, MemSize, 
CASE	
	WHEN AMPS > 0 THEN MaxPerm_ 
	ELSE 0  
END	AS AMPSize 
FROM(
SELECT	DISTINCT(NodeID) AS NodeId, Nodetype AS Model, NCPUs AS NCPU,
		VPROC1 AS AMPs,  VPROC2 AS PEs, VPROC3 AS GTW, MemSize AS MemSize,
		thedate AS Logdate 
FROM	DBC.ResUsageSPMA 
WHERE	Logdate = DATE) PMA INNER JOIN(
SELECT	DATE AS Logdate,  VPROC, SUM(CURRENTPERM) AS CurrentPerm,
		SUM(PEAKPERM) AS PeakPerm, SUM(MAXPERM) AS MaxPerm_, (AVG(a.CURRENTPERM) / NULLIFZERO(MAX(a.CURRENTPERM))) * 100 AS CurrentPermSkew,
		 (SUM(CURRENTPERM) / NULLIFZERO(SUM(MAXPERM))) * 100 AS PermPctUsed 
FROM	DBC.DISKSPACE a 
WHERE	 a.maxPERM > 0 
GROUP BY 1, 2) DS 
	ON PMA.Logdate = DS.Logdate 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9) dt 
GROUP BY 1, 2, 3) dt1;