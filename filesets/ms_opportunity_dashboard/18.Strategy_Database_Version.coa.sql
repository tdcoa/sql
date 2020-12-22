
LOCKING ROW FOR ACCESS 
SELECT	DATE(FORMAT 'yyyy-mm-dd') logdate, infodata  db_version 
FROM	dbc.dbcinfo 
WHERE	 infokey = 'VERSION';
