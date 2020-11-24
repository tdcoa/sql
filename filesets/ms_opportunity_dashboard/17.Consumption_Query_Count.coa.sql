LOCKING	ROW FOR ACCESS 
SELECT	logdate(FORMAT 'yyyy-mm-dd') logdate, COUNT(queryid)  query_count 
FROM	pdcrinfo.DBQLogTbl_hst 
WHERE	logdate >= DATE - 90 
GROUP BY logdate 
ORDER BY logdate;
