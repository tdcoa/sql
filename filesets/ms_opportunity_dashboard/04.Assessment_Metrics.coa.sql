CREATE	MULTISET VOLATILE TABLE temp_logtbl AS( 
SELECT	b.ObjectDatabaseName AS DBNAME, b.ObjectTableName AS TbName,
		a.statementtype AS SmtType, NumofActiveAmps AS ActiveAmp, SUM(a.NumofActiveAmps * a.MaxAMPCPUTime) AS ImpactCPU,
		COUNT(a.queryid) Counts, SUM(a.AMPCPUTime + a.ParserCPUTime) AS TotalCPU,
		SUM(a.TotalIOCount) AS TotalIO  
FROM	pdcrinfo.dbqlogtbl_hst a INNER JOIN pdcrinfo.dbqlobjtbl_hst b 
	ON a.procid = b.procid 
	AND a.queryid = b.queryid  
	AND a.logdate = b.logdate 
	AND a.logdate BETWEEN DATE - 30 
	AND DATE - 1 
WHERE	b.objecttype = 'Tab'  
GROUP BY 1, 2, 3, 4) 
WITH	DATA PRIMARY INDEX (DBNAME, TbName) 
	ON 
COMMIT	PRESERVE ROWS;

LOCKING ROW FOR ACCESS 
SELECT	( 
SELECT	SUM(CAST ((b.tablesize/ (1024*1024*1024)) AS DECIMAL(38,
		2))) AS  TableSize_in_GB  
FROM	  (
SELECT	a.DatabaseName, a.TABLENAME, SUM(
CASE	
	WHEN a.Compressible = 'C' THEN 1 
	ELSE 0 
END) AS Compressed,  SUM(
CASE	
	WHEN a.Compressible = 'N' THEN 1 
	ELSE 0 
END) AS NotCompressed 
FROM	dbc.COLUMNSV a INNER JOIN dbc.tablesV c  
	ON a.DatabaseName = c.DatabaseName 
	AND a.TABLENAME = c.TABLENAME 
WHERE	c.TableKind = 'T' 
	AND columntype  NOT IN('BO', 'CO', 'PD', 'PM', 'PS', 'PT', 'PZ',
		'UT') 
	AND(a.databasename, a.TABLENAME, a.columnname) NOT IN(
SELECT	 databasename, TABLENAME, columnname 
FROM	dbc.indicesV 
WHERE	indextype IN('P', 'Q')) 
	AND(A.databasename)  NOT IN('spool_reserve', 'spoolreserve', '$NETVAULT_CATALOG',
		'All', 'console', 'Crashdumps', 'DBC', 'DBCMANAGER', 'dbcmngr',
		'dbcmngr12',  'dbqm', 'dbqrymgr', 'Default', 'HIGA', 'NETVAULT1',
		'PUBLIC', 'SQLJ', 'Sys_Calendar', 'SYS_MGMT', 'SYSLIB', 'SYSSPATIAL',
		'SystemFe',  'SYSUDTLIB', 'SYSUSR', 'TD_SYSFNLIB', 'TMADMIN',
		'viewpoint') 
	AND((A.databasename)NOT LIKE ALL('%QCD%', '%tdwm%', '%tswiz%',
		 '%twm%', '%PDCR%', '%PMCP%', '%twm%') 
	AND TRIM(A.databasename) NOT LIKE ALL('abcdef')) 
	AND TRIM(A.databasename) NOT IN  ('abcdef') 
GROUP BY 1, 2 
HAVING	Compressed = 0) a INNER JOIN(
SELECT	databasename, TABLENAME, SUM(currentperm) AS  TableSize 
FROM	dbc.tablesizeV 
GROUP BY 1, 2) b 
	ON a.Databasename = b.databasename 
	AND a.TABLENAME = b.TABLENAME  INNER JOIN(
SELECT	databasename, TABLENAME, COUNT(columnname) AS TotColumns 
FROM	dbc.COLUMNSV 
GROUP BY 1, 2) cols  
	ON a.databasename = cols.databasename 
	AND a.TABLENAME = cols.TABLENAME) AS Tables_with_no_MVC_GB,  (
SELECT	CAST(SUM(currentperm) / (1024 * 1024 * 1024) AS DECIMAL(12,
		2)) AS CurrPerm_GB  
FROM	  DBC.Diskspacev ) AS CurrPerm_GB,  (
SELECT	CAST(SUM(maxperm) / (1024 * 1024 * 1024) AS DECIMAL(12,
		2)) AS MaxPerm_GB 
FROM	  DBC.Diskspacev ) AS MaxPerm_GB,  (
SEL	AVG(suspectCPUpct) 
FROM(
SELECT	EXTRACT(MONTH FROM logdate) AS Mnth, AVG(sumcpu) AS Avg_CPU,
		AVG(SuspectCPU)  AS AVGSuspectCPU, (AVGSuspectCPU / Avg_CPU) * 100 suspectCPUpct 
FROM(
SELECT	LogDate, SUM(AMPCPUTime + Parsercputime)  SumCPU, SUM(
CASE	
	WHEN(TotalIOCount > 0 
	AND((AMPCPUTime + Parsercputime) * 1000) / TotalIOCount > 3 /* PJI */) 
	OR ((AMPCPUTime + Parsercputime) > 0 
	AND((TotalIOCount) / ((AMPCPUTime + Parsercputime) * 1000) > 3 /* UII */)) 
	OR(((AMPCPUTime + Parsercputime) / (HASHAMP() + 1)) > 0 
	AND  (1 - (AmpCPUTime / (HASHAMP() + 1)) / NULLIFZERO(MaxAmpCPUTime)) > 0.5 /* CPU Skew */) 
	OR((TotalIOCount / (HASHAMP() + 1)) > 0 
	AND  (1 - (TotalIOCount / (HASHAMP() + 1)) / NULLIFZERO(MaxAmpIO)) > 0.5 /* IO Skew */) THEN(AMPCPUTime + Parsercputime) 
	ELSE 0 
END)  SuspectCPU, MAX(UserCPUPerNode) UserCPUPerNode, MAX(TotalUserCPU) TotalUserCPU 
FROM	pdcrinfo.dbqlogtbl_hst a,  (
SELECT	NodeType, NCPUs*86400 * .8 AS UserCPUPerNode, COUNT(DISTINCT(NodeID)) AS Nodes,
		UserCPUPerNode * Nodes AS  TotalUserCPU 
FROM	DBC.ResUsageSPMA 
WHERE	thedate = DATE 
	AND vproc1 > 0 
GROUP BY 1, 2) cpuinfo 
WHERE	 logdate BETWEEN DATE - 30 
	AND DATE - 1 
GROUP BY 1) a 
GROUP BY 1) f ) AS suspectCPUpct, (
SELECT	COUNT(*) AS To_sec_idx  
FROM(
SELECT	TRIM(DBCI.DatabaseName) AS DatabaseName, TRIM(DBCI.TABLENAME) AS TABLENAME,
		DBCI.indextype AS Type_ID,  
CASE	DBCI.indextype 
	WHEN 'P' THEN 'Nonpartitioned Primary' 
	WHEN 'Q' THEN 'Partitioned Primary' 
	WHEN 'S' THEN 'Secondary' 
	WHEN 'K' THEN 'Primary Key'  
	WHEN 'U' THEN 'Unique Constraint' 
	WHEN 'V' THEN 'Value Ordered Secondary' 
	WHEN 'H' THEN 'Hash Ordered ALL Covering Secondary'  
	WHEN 'O' THEN 'Value Ordered ALL Covering Secondary' 
	WHEN 'I' THEN 'Ordering Column of a Composite Secondary Index'  
	WHEN 'M' THEN 'Multi-Column Statistics' 
	WHEN 'D' THEN 'Derived Column Partition Statistics' 
	WHEN '1' THEN 'Hash Index'  
	WHEN '2' THEN 'Hash Index' 
	ELSE 'Unknown' 
END	AS Index_Type, TRIM(DBCI.IndexNumber) AS IdxNum, TRIM(DBCI.IndexName)  AS IdxName,
		CAST(DBCI.TableSize AS DECIMAL(30, 0)) AS TableSize, ZEROIFNULL(CAST(UsageCnt AS DECIMAL(18,
		3))) AS IdxUsage,  ZEROIFNULL(CAST((UsageCnt * 100) / TableCnt AS DECIMAL(18,
		3))) AS IdxUsagePct, ZEROIFNULL(CAST(TableCnt AS DECIMAL(18,
		3))) AS TableUsage,  NumLds, DBCI.ColName 
FROM(
SELECT	i.IndexType, I.IndexNumber, I.IndexName, i.databasename,
		i.TABLENAME, SIZE.TableSize,  MAX(
CASE	
	WHEN i.ColumnPosition = 1 THEN TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 2 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 3  THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 4 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	 
	WHEN i.ColumnPosition = 5 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 6 THEN ',' || TRIM(i.ColumnName)  
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 7 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 8  THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 9 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	 
	WHEN i.ColumnPosition = 10 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 11 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	 
	WHEN i.ColumnPosition = 12 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 13 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	 
	WHEN i.ColumnPosition = 14 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 15 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	 
	WHEN i.ColumnPosition = 16 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition > 16 THEN ',...' 
	ELSE '' 
END) AS ColName  
FROM	  DBC.INDICESv I INNER JOIN(
SELECT	databasename, TABLENAME, SUM(CurrentPerm) AS TableSize 
FROM	  dbc.tablesizev  
GROUP BY 1, 2) SIZE 
	ON I.databasename = SIZE.databasename 
	AND I.TABLENAME = SIZE.TABLENAME 
WHERE	i.indextype IS NOT  NULL 
	AND i.indextype NOT IN('P', 'K', 'Q', 'M', 'J', 'N', '1', '2') 
	AND I.databasename NOT IN('spool_reserve', 'spoolreserve', '$NETVAULT_CATALOG',
		'All', 'console', 'Crashdumps', 'DBC',  'DBCMANAGER', 'dbcmngr',
		'dbcmngr12', 'dbqm', 'dbqrymgr', 'Default', 'HIGA', 'NETVAULT1',
		'PUBLIC', 'SQLJ', 'Sys_Calendar', 'SYS_MGMT',  'SYSLIB', 'SYSSPATIAL',
		'SystemFe', 'SYSUDTLIB', 'SYSUSR', 'TD_SYSFNLIB', 'TMADMIN',
		'viewpoint') 
	AND I.databasename NOT LIKE  ALL('%PDCR%', '%PMCP%', '%QCD%',
		'%tdwm%', '%tswiz%', '%twm%') 
	AND(TRIM(I.databasename) NOT IN('abcdef') 
	AND  TRIM(I.databasename) NOT LIKE ALL('abcdef')) 
GROUP BY 1, 2, 3, 4, 5, 6) DBCI LEFT OUTER JOIN(
SELECT	X.objdbname, objtbname,  objnum, CAST(SUM(freqofuse) AS DECIMAL(18,
		4)) 
FROM(
SELECT	obj.objdbname, obj.objtbname, obj.objnum, idxdbname, idxtbname,
		 idxnum, idxcolpos, objfreqofuse, 
CASE	
	WHEN idxcolpos IS NULL THEN objfreqofuse 
	ELSE objfreqofuse / idxcolpos 
end	AS freqofuse  
FROM(
SELECT	TRIM(objectdatabasename) objdbname, TRIM(objecttablename) objtbname,
		TRIM(objectcolumnname) objcolname,  TRIM(objectNum) objnum, CAST(SUM(freqofuse) AS DECIMAL(18,
		4)) AS objfreqofuse 
FROM	pdcrinfo.dbqlobjtbl_hst o, pdcrinfo.dbqlogtbl_hst a 
WHERE	 o.queryid = a.queryid 
	AND o.logdate = a.logdate 
	AND statementtype NOT LIKE '%Statistics%' 
	AND statementtype NOT LIKE '%Mload%'  
	AND statementtype NOT IN('Commit Work', 'Begin Transaction', 'End Transaction',
		'End Edit', 'Help', 'Database', 'Null', 'Call',  'Comment Get/Set',
		'Set Session', 'Echo', 'Drop Table', 'Replace View', 'Modify Database',
		'Drop View', 'Alter Table', 'Rename Table',  'Create View', 'Replace Macro',
		'Create Macro', 'Create Database/User', 'Drop Macro', 'Drop Database',
		'Delete Database', 'Replace Procedure', 'Rename View', 'CheckPoint Loading',
		 'Release lock', 'Begin Loading', 'End Loading', 'Check Point',
		'Grant', 'Revoke', 'Give', 'Create Role', 'Create Index', 'Drop Index',
		 'Create Join Index', 'Drop Hash Index', 'Create Hash Index') 
	AND objecttype = 'Idx' 
	AND o.logdate BETWEEN DATE - 30  
	AND DATE - 1 
GROUP BY 1, 2, 3, 4) obj LEFT OUTER JOIN(
SELECT	databasename AS idxdbname, TABLENAME AS idxtbname,  indexnumber AS idxnum,
		MAX(columnposition) AS idxcolpos 
FROM	  dbc.indicesv  
GROUP BY 1, 2, 3) idx 
	ON objdbname = idxdbname  
	AND objtbname = idxtbname 
	AND objnum = idxnum 
	AND objcolname IS NOT NULL) AS X 
GROUP BY 1, 2, 3) DBQI(DBNAME, TName,  IdxNum, UsageCnt) 
	ON TRIM(DBCI.DatabaseName) = DBQI.DBNAME 
	AND TRIM(DBCI.TABLENAME) = DBQI.TName 
	AND  TRIM(DBCI.Indexnumber) = DBQI.IdxNum LEFT OUTER JOIN(
SELECT	TRIM(objectdatabasename) AS DBNAME, TRIM(objecttablename)  AS TName,
		SUM(
CASE	
	WHEN statementtype IN('Execute Mload', 'End Loading', 'Insert') THEN 1 
	ELSE 0 
end) AS NumLds,  CAST(SUM(freqofuse) AS DECIMAL(18, 4)) AS Tablecnt 
FROM	pdcrinfo.dbqlobjtbl_hst o, pdcrinfo.dbqlogtbl_hst a 
WHERE	o.queryid = a.queryid  
	AND o.logdate = a.logdate 
	AND statementtype NOT LIKE '%Statistics%' 
	AND statementtype NOT LIKE '%Mload%' 
	AND statementtype  NOT IN('Commit Work', 'Begin Transaction',
		'End Transaction', 'End Edit', 'Help', 'Database', 'Null', 'Call',
		'Comment Get/Set', 'Set Session',  'Echo', 'Drop Table', 'Replace View',
		'Modify Database', 'Drop View', 'Alter Table', 'Rename Table',
		'Create View', 'Replace Macro',  'Create Macro', 'Create Database/User',
		'Drop Macro', 'Drop Database', 'Delete Database', 'Replace Procedure' 'Rename View',
		 'CheckPoint Loading', 'Release lock', 'Begin Loading', 'End Loading',
		'Check Point', 'Grant', 'Revoke', 'Give', 'Create Role', 'Create Index',
		 'Drop Index', 'Create Join Index', 'Drop Hash Index', 'Create Hash Index') 
	AND objecttype = 'Tab' 
	AND o.logdate BETWEEN DATE - 30  
	AND DATE - 1 
GROUP BY 1, 2) DBQT 
	ON TRIM(DBCI.DatabaseName) = DBQT.DBNAME 
	AND TRIM(DBCI.TABLENAME) = DBQT.TName)a)  AS To_sec_idx,  (
SELECT	SUM(
CASE	
	WHEN idxusagepct = 0.000 THEN 1 
	ELSE 0 
end) AS Tot_sec_idx_notusd 
FROM	 (
SELECT	TRIM(DBCI.DatabaseName) AS DatabaseName, TRIM(DBCI.TABLENAME) AS TABLENAME,
		DBCI.indextype AS Type_ID,  
CASE	DBCI.indextype 
	WHEN 'P' THEN 'Nonpartitioned Primary' 
	WHEN 'Q' THEN 'Partitioned Primary' 
	WHEN 'S' THEN 'Secondary'  
	WHEN 'K' THEN 'Primary Key' 
	WHEN 'U' THEN 'Unique Constraint' 
	WHEN 'V' THEN 'Value Ordered Secondary' 
	WHEN 'H'  THEN 'Hash Ordered ALL Covering Secondary' 
	WHEN 'O' THEN 'Value Ordered ALL Covering Secondary' 
	WHEN 'I'  THEN 'Ordering Column of a Composite Secondary Index' 
	WHEN 'M' THEN 'Multi-Column Statistics' 
	WHEN 'D'  THEN 'Derived Column Partition Statistics' 
	WHEN '1' THEN 'Hash Index' 
	WHEN '2' THEN 'Hash Index' 
	ELSE 'Unknown' 
END	 AS Index_Type, TRIM(DBCI.IndexNumber) AS IdxNum, TRIM(DBCI.IndexName) AS IdxName,
		CAST(DBCI.TableSize AS DECIMAL(30, 0))  AS TableSize, ZEROIFNULL(CAST(UsageCnt AS DECIMAL(18,
		3))) AS IdxUsage, ZEROIFNULL(CAST((UsageCnt * 100) / TableCnt AS DECIMAL(18,
		3)))  AS IdxUsagePct, ZEROIFNULL(CAST(TableCnt AS DECIMAL(18,
		3))) AS TableUsage, NumLds, DBCI.ColName 
FROM(
SELECT	i.IndexType,  I.IndexNumber, I.IndexName, i.databasename,
		i.TABLENAME, SIZE.TableSize, MAX(
CASE	
	WHEN i.ColumnPosition = 1 THEN  TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 2 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	 
	WHEN i.ColumnPosition = 3 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 4 THEN ',' || TRIM(i.ColumnName)  
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 5 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 6  THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 7 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	 
	WHEN i.ColumnPosition = 8 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 9 THEN ',' || TRIM(i.ColumnName)  
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 10 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 11  THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 12 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	 
	WHEN i.ColumnPosition = 13 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 14 THEN ',' || TRIM(i.ColumnName)  
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 15 THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition = 16  THEN ',' || TRIM(i.ColumnName) 
	ELSE '' 
END) || MAX(
CASE	
	WHEN i.ColumnPosition > 16 THEN ',...' 
	ELSE '' 
END) AS ColName 
FROM	  DBC.INDICESv I  INNER JOIN(
SELECT	databasename, TABLENAME, SUM(CurrentPerm) AS TableSize 
FROM	  dbc.tablesizev 
GROUP BY 1, 2) SIZE 
	ON I.databasename =  SIZE.databasename 
	AND I.TABLENAME = SIZE.TABLENAME 
WHERE	i.indextype IS NOT NULL 
	AND i.indextype NOT IN('P', 'K', 'Q', 'M', 'J', 'N', '1', '2')  
	AND I.databasename NOT IN('spool_reserve', 'spoolreserve', '$NETVAULT_CATALOG',
		'All', 'console', 'Crashdumps', 'DBC', 'DBCMANAGER', 'dbcmngr',
		 'dbcmngr12', 'dbqm', 'dbqrymgr', 'Default', 'HIGA', 'NETVAULT1',
		'PUBLIC', 'SQLJ', 'Sys_Calendar', 'SYS_MGMT', 'SYSLIB', 'SYSSPATIAL',
		 'SystemFe', 'SYSUDTLIB', 'SYSUSR', 'TD_SYSFNLIB', 'TMADMIN',
		'viewpoint') 
	AND I.databasename NOT LIKE ALL('%PDCR%', '%PMCP%', '%QCD%',  '%tdwm%',
		'%tswiz%', '%twm%') 
	AND(TRIM(I.databasename) NOT IN('abcdef') 
	AND TRIM(I.databasename) NOT LIKE ALL('abcdef'))  
GROUP BY 1, 2, 3, 4, 5, 6) DBCI LEFT OUTER JOIN(
SELECT	X.objdbname, objtbname, objnum, CAST(SUM(freqofuse) AS DECIMAL(18,
		4))  
FROM(
SELECT	obj.objdbname, obj.objtbname, obj.objnum, idxdbname, idxtbname,
		idxnum, idxcolpos, objfreqofuse, 
CASE	 
	WHEN idxcolpos IS NULL THEN objfreqofuse 
	ELSE objfreqofuse / idxcolpos 
end	AS freqofuse 
FROM(
SELECT	 TRIM(objectdatabasename) objdbname, TRIM(objecttablename) objtbname,
		TRIM(objectcolumnname) objcolname, TRIM(objectNum) objnum,  CAST(SUM(freqofuse) AS DECIMAL(18,
		4)) AS objfreqofuse 
FROM	pdcrinfo.dbqlobjtbl_hst o, pdcrinfo.dbqlogtbl_hst a  
WHERE	o.queryid = a.queryid 
	AND o.logdate = a.logdate 
	AND statementtype NOT LIKE '%Statistics%' 
	AND statementtype NOT  LIKE '%Mload%' 
	AND statementtype NOT IN('Commit Work', 'Begin Transaction', 'End Transaction',
		'End Edit', 'Help', 'Database', 'Null',  'Call', 'Comment Get/Set',
		'Set Session', 'Echo', 'Drop Table', 'Replace View', 'Modify Database',
		'Drop View', 'Alter Table', 'Rename Table',  'Create View', 'Replace Macro',
		'Create Macro', 'Create Database/User', 'Drop Macro', 'Drop Database',
		'Delete Database', 'Replace Procedure',  'Rename View', 'CheckPoint Loading',
		'Release lock', 'Begin Loading', 'End Loading', 'Check Point',
		'Grant', 'Revoke', 'Give', 'Create Role',  'Create Index', 'Drop Index',
		'Create Join Index', 'Drop Hash Index', 'Create Hash Index') 
	AND objecttype = 'Idx' 
	AND o.logdate  BETWEEN DATE - 30 
	AND DATE - 1 
GROUP BY 1, 2, 3, 4) obj LEFT OUTER JOIN(
SELECT	databasename AS idxdbname,  TABLENAME AS idxtbname, indexnumber AS idxnum,
		MAX(columnposition) AS idxcolpos 
FROM	  dbc.indicesv  
GROUP BY 1, 2, 3) idx  
	ON objdbname = idxdbname 
	AND objtbname = idxtbname 
	AND objnum = idxnum 
	AND objcolname IS NOT NULL) AS X 
GROUP BY 1, 2, 3)  DBQI(DBNAME, TName, IdxNum, UsageCnt) 
	ON TRIM(DBCI.DatabaseName) = DBQI.DBNAME 
	AND TRIM(DBCI.TABLENAME) = DBQI.TName  
	AND TRIM(DBCI.Indexnumber) = DBQI.IdxNum LEFT OUTER JOIN(
SELECT	TRIM(objectdatabasename) AS DBNAME,  TRIM(objecttablename) AS TName,
		SUM(
CASE	
	WHEN statementtype IN('Execute Mload', 'End Loading', 'Insert') THEN 1 
	ELSE 0 
end)  AS NumLds, CAST(SUM(freqofuse) AS DECIMAL(18, 4)) AS Tablecnt 
FROM	pdcrinfo.dbqlobjtbl_hst o, pdcrinfo.dbqlogtbl_hst a  
WHERE	o.queryid = a.queryid 
	AND o.logdate = a.logdate 
	AND statementtype NOT LIKE '%Statistics%' 
	AND statementtype NOT  LIKE '%Mload%' 
	AND statementtype NOT IN('Commit Work', 'Begin Transaction', 'End Transaction',
		'End Edit', 'Help', 'Database', 'Null', 'Call',  'Comment Get/Set',
		'Set Session', 'Echo', 'Drop Table', 'Replace View', 'Modify Database',
		'Drop View', 'Alter Table', 'Rename Table',  'Create View', 'Replace Macro',
		'Create Macro', 'Create Database/User', 'Drop Macro', 'Drop Database',
		'Delete Database', 'Replace Procedure', 'Rename View',  'CheckPoint Loading',
		'Release lock', 'Begin Loading', 'End Loading', 'Check Point',
		'Grant', 'Revoke', 'Give', 'Create Role', 'Create Index', 'Drop Index',
		 'Create Join Index', 'Drop Hash Index', 'Create Hash Index') 
	AND objecttype = 'Tab' 
	AND o.logdate BETWEEN DATE - 30 
	AND DATE - 1  
GROUP BY 1, 2) DBQT 
	ON TRIM(DBCI.DatabaseName) = DBQT.DBNAME 
	AND TRIM(DBCI.TABLENAME) = DBQT.TName)a) AS  Tot_sec_idx_notusd,
		(
SEL	COUNT(*) 
FROM(
SEL	a.DatabaseName, a.TABLENAME, a.ColumnName expressionlist, a.rowcount,
		a.uniquevaluecount, (a.uniquevaluecount) / a.rowcount * 100  AS uniquenesspct,
		a.nullcount, a.samplesizepct, a.LastAlterTimeStamp 
FROM	dbc.statsV a) c JOIN(
SEL	databasename, TABLENAME,  SUM(currentperm) AS tablespace                    
FROM	dbc.tablesizev 
GROUP BY 1, 2) d 
	ON c.databasename = d.databasename 
	AND  c.TABLENAME = d.TABLENAME LEFT JOIN(
SEL	b.objectdatabasename, b.objecttablename, (SUBSTRING(a.sqltextinfo 
FROM	0 FOR 1000))  AS SQL_Text, SUM(ampcputime) AS sumcpu, COUNT(*) AS collect_counts 
FROM	pdcrinfo.dbqlsqltbl_hst a JOIN(
SEL	DISTINCT a.procid,  a.queryid, a.logdate, objectdatabasename,
		objecttablename, ampcputime 
FROM	pdcrinfo.dbqlobjtbl_hst a JOIN pdcrinfo.dbqlogtbl_hst c  
	ON a.procid = c.procid 
	AND a.queryid = c.queryid 
	AND a.logdate = c.logdate 
WHERE	c.statementtype LIKE '%collect stat%' 
	AND a.logdate   BETWEEN DATE - 30 
	AND DATE - 1) b 
	ON a.procid = b.procid 
	AND a.queryid = b.queryid 
	AND a.logdate = b.logdate 
WHERE	a.logdate   BETWEEN DATE - 30 
	AND DATE - 1 
HAVING	sumcpu > 10000 
GROUP BY 1, 2, 3) e 
	ON e.objectdatabasename = d.databasename 
	AND  e.objecttablename = d.TABLENAME 
WHERE	rowcount > 0 
	AND expressionlist IS NOT NULL 
	AND UniquenessPct > 95 ) AS UNIQ_GT_95,  (
SEL	COUNT(*) 
FROM(
SEL	a.databasename, a.TABLENAME, a.columnname expressionlist, a.rowcount,
		a.uniquevaluecount,  (a.uniquevaluecount) / a.rowcount * 100 AS uniquenesspct,
		a.nullcount, a.samplesizepct, a.LastAlterTimeStamp, samplesignature 
FROM	 dbc.statsV a) c JOIN(
SEL	databasename, TABLENAME, SUM(currentperm) AS tablespace                    
FROM	dbc.tablesizev 
GROUP BY 1, 2) d  
	ON c.databasename = d.databasename 
	AND c.TABLENAME = d.TABLENAME LEFT JOIN(
SEL	b.objectdatabasename, b.objecttablename, (SUBSTRING(a.sqltextinfo  
FROM	0 FOR 1000)) AS SQL_Text, SUM(ampcputime) AS sumcpu, COUNT(*) AS collect_counts 
FROM	pdcrinfo.dbqlsqltbl_hst a  JOIN(
SEL	DISTINCT a.procid, a.queryid, a.logdate, objectdatabasename,
		objecttablename, ampcputime 
FROM	pdcrinfo.dbqlobjtbl_hst a  JOIN pdcrinfo.dbqlogtbl_hst c 
	ON a.procid = c.procid 
	AND a.queryid = c.queryid 
	AND a.logdate = c.logdate 
WHERE	c.statementtype LIKE '%collect stat%'  
	AND a.logdate BETWEEN DATE - 30 
	AND DATE - 1) b 
	ON a.procid = b.procid 
	AND a.queryid = b.queryid 
	AND a.logdate = b.logdate  
WHERE	a.logdate BETWEEN DATE - 30 
	AND DATE - 1 
HAVING	sumcpu > 10000 
GROUP BY 1, 2, 3) e 
	ON e.objectdatabasename = d.databasename  
	AND e.objecttablename = d.TABLENAME 
WHERE	rowcount > 0 
	AND expressionlist IS NOT NULL 
	AND UniquenessPct > 95 
	AND  samplesignature IN('USP00nn.00', 'SDPxxxx.xx') ) AS sample_defined,
		(
SELECT	SUM(b.TotalCPU) AS TotalCPU 
FROM	temp_logtbl b  
WHERE(b.DBNAME, b.TbName) IN(
SELECT	b.DBNAME, b.TbName 
FROM	temp_logtbl b 
WHERE	b.SmtType = 'Collect Statistics'  
GROUP BY 1, 2 
MINUS	
SELECT	b.DBNAME, b.TbName 
FROM	temp_logtbl b 
WHERE	b.SmtType NOT IN('Collect Statistics') 
GROUP BY 1, 2)  
	AND b.DBNAME NOT IN('spool_reserve', 'spoolreserve', '$NETVAULT_CATALOG',
		'All', 'console', 'Crashdumps', 'DBC', 'DBCMANAGER', 'dbcmngr',
		 'dbcmngr12', 'dbqm', 'dbqrymgr', 'Default', 'HIGA', 'NETVAULT1',
		'PUBLIC', 'SQLJ', 'Sys_Calendar', 'SYS_MGMT', 'SYSLIB', 'SYSSPATIAL',
		'SystemFe',  'SYSUDTLIB', 'SYSUSR', 'TD_SYSFNLIB', 'TMADMIN',
		'viewpoint') 
	AND b.DBNAME NOT LIKE ALL('%PDCR%', '%PMCP%', '%QCD%', '%tdwm%',
		'%tswiz%', '%twm%') )  Unwanted_Stats_CPU , (
SELECT	SUM(b.TotalCPU) AS TotalCPU 
FROM	temp_logtbl b 
WHERE	b.SmtType = 'Collect Statistics')   Total_CPU , (
SEL	COUNT(*) 
FROM	dbc.tablesv 
WHERE	tablekind = 't' 
	AND TRIM(DatabaseName) || '.' || TRIM(TABLENAME) IN  (
SELECT	DISTINCT TRIM(DBNAME) || '.' || TRIM(tbname) 
FROM	temp_logtbl)) AS Total_Tables_Accessed, (
SEL	COUNT(*) 
FROM	 (
SELECT	DISTINCT a.DBNAME AS DatabaseName, a.TbName AS TABLENAME 
FROM	temp_logtbl a 
WHERE	a.SmtType = 'SELECT'  
	AND(a.DBNAME, a.TbName) IN(
SELECT	DATABASENAME, TABLENAME 
FROM	dbc.TablesV TVM 
WHERE	TVM.TableKind IN('T', 'I')  
	AND databasename NOT IN('spool_reserve', 'spoolreserve', '$NETVAULT_CATALOG',
		'All', 'console', 'Crashdumps', 'DBC', 'DBCMANAGER',  'dbcmngr',
		'dbcmngr12', 'dbqm', 'dbqrymgr', 'Default', 'HIGA', 'NETVAULT1',
		'PUBLIC', 'SQLJ', 'Sys_Calendar', 'SYS_MGMT', 'SYSLIB', 'SYSSPATIAL',
		 'SystemFe', 'SYSUDTLIB', 'SYSUSR', 'TD_SYSFNLIB', 'TMADMIN',
		'viewpoint') 
	AND databasename NOT LIKE ALL('%PDCR%', '%PMCP%', '%QCD%',  '%tdwm%',
		'%tswiz%', '%twm%') 
MINUS(
SELECT	DatabaseName, TABLENAME 
FROM	dbc.statsv))) a) AS Total_Missing_stats, (
SELECT	COUNT(*) 
FROM	 (
SELECT	a.Databasename, a.TABLENAME 
FROM	dbc.tablesV a, (
SELECT	databasename, TABLENAME, SUM(
CASE	
	WHEN  uniqueFlag = 'Y' THEN 1 
	ELSE 0 
END) AS Equals0MeansNU 
FROM	dbc.indicesV 
GROUP BY 1, 2 
HAVING	Equals0MeansNU = 0) b  
WHERE	a.databasename = b.databasename 
	AND a.TABLENAME = b.TABLENAME 
	AND a.tablekind = 'T' 
	AND a.checkopt = 'N'  
	AND a.databasename NOT IN('spool_reserve', 'spoolreserve', '$NETVAULT_CATALOG',
		'All', 'console', 'Crashdumps', 'DBC', 'DBCMANAGER', 'dbcmngr',
		 'dbcmngr12', 'dbqm', 'dbqrymgr', 'Default', 'HIGA', 'NETVAULT1',
		'PUBLIC', 'SQLJ', 'Sys_Calendar', 'SYS_MGMT', 'SYSLIB', 'SYSSPATIAL',
		'SystemFe', 'SYSUDTLIB', 'SYSUSR',  'TD_SYSFNLIB', 'TMADMIN',
		'viewpoint', 'DBCEXTENSION') 
	AND a.databasename NOT LIKE ALL ('%PDCR%', '%PMCP%', '%QCD%',
		'%tdwm%', '%tswiz%', '%twm%') )  AS A) SET_TBL_CNT; ;

DROP TABLE temp_logtbl;