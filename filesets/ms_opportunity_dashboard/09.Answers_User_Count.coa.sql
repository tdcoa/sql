LOCKING	ROW FOR ACCESS 
SEL	logdate,COUNT(DISTINCT USERNAME) 
FROM	pdcrinfo.logonoff_hst 
WHERE	logdate >=DATE-90 
GROUP BY 1;
