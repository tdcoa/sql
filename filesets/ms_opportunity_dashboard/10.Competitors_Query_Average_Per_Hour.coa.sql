LOCKING	ROW FOR ACCESS 
SELECT	logdate(FORMAT 'yyyy-mm-dd'),(CAST(CAST(COUNT(queryid) AS FLOAT) /24 AS FLOAT)) queryAverage 
FROM	pdcrinfo.DBQLogTbl_hst 
WHERE	logdate >=DATE-90 
GROUP BY logdate 
ORDER BY logdate;
