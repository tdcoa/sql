LOCKING	ROW FOR ACCESS 
SELECT	LogDate,SUM(CurrentPerm) /(1024*1024*1024)  AS current_perm_in_gb,
		SUM(MaxPerm)/(1024*1024*1024) AS max_perm_in_gb ,max_perm_in_gb-current_perm_in_gb AS available_perm_in_gb,
		((current_perm_in_gb / max_perm_in_gb) * 100)(DECIMAL(5, 2))AS Space_consumed_pct 
FROM	 PDCRInfo.DataBaseSpace_Hst 
WHERE	  logdate >= DATE - 90 
GROUP BY 1;
