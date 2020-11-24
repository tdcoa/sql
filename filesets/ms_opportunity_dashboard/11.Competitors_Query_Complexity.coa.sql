LOCKING	ROW FOR ACCESS 
SELECT	logdate(FORMAT 'yyyy-mm-dd'),COUNT(logdate) query_complexity 
FROM(
SELECT	logdate, queryid, COUNT(stepname) AS step 
FROM	pdcrinfo.dbqlsteptbl_hst 
WHERE	logdate >= DATE - 90 
	AND stepname = 'JIN' 
GROUP BY logdate, queryid 
HAVING	step > 5) results 
GROUP BY logdate;
