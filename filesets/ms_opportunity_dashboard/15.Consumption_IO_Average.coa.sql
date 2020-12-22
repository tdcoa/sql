 LOCKING ROW FOR ACCESS 
SELECT	logdate(FORMAT 'yyyy-mm-dd') logdate ,AVG(totaliocount)  io_average 
FROM	 pdcrinfo.dbqlogtbl_hst 
WHERE	logdate >= DATE - 90 
GROUP BY logdate 
ORDER BY logdate;
