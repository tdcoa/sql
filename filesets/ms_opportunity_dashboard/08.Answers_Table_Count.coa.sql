LOCKING	ROW FOR ACCESS 
SELECT	logdate,COUNT(TABLENAME) 
FROM	pdcrinfo.tablespace_hst 
WHERE	logdate >=DATE-90 
GROUP BY 1;
