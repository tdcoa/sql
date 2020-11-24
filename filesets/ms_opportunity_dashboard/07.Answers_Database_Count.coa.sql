LOCKING	ROW FOR ACCESS 
SELECT	logdate,COUNT(databasename) 
FROM	pdcrinfo.DatabaseSpace_Hst 
WHERE	logdate >=DATE-90 
GROUP BY 1;
