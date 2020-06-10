SELECT C.SubDepartment,
	   C.Department,
	   FeatureName,
	   ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 - B.FEATUREBITPOS)))) AS FeatureUseCount 
FROM PDCRINFO.DbqlogTbl A
CROSS JOIN (
SELECT CASE WHEN B.FEATURENAME LIKE '%Partition%' THEN 'Partitioning'
	   		WHEN B.FEATURENAME LIKE '%DBQL%' THEN 'Logging'
			WHEN B.FEATURENAME LIKE '%LOB%' THEN 'LOB'
			WHEN B.FEATURENAME LIKE '%Geospatial%' THEN 'Geospatial'
			WHEN B.FEATURENAME LIKE '%QueryGrid%' THEN 'QueryGrid'
			WHEN B.FEATURENAME LIKE '%In-Memory%' THEN 'In-Memory Query Optimization'
			WHEN B.FEATURENAME LIKE '%Dataset Data Type Storage Format CSV%' THEN 'CSV Data Type'
			WHEN B.FEATURENAME LIKE ANY ('%MloadX Protocol%','%Multiload Protocol%') THEN 'MultiLoad'
			WHEN B.FEATURENAME LIKE ANY ('%Non Unique Hashed Index%','%Non Unique UnHashed Index%') THEN 'Hashed Index'
			WHEN B.FEATURENAME LIKE '%Dynamic Parameterization of Literals Feature%' THEN 'Dynamic Parameterization'
			WHEN B.FEATURENAME LIKE '%LOB%' THEN 'LOB Data Type'
			ELSE B.FEATURENAME END as FeatureName,
		FEATUREBITPOS
FROM DBC.QRYLOGFEATURELISTV B
WHERE B.FEATURENAME NOT IN (
'Dataset Data Type',
'Distinct Data Type',
'Dot Notation',
'Incremental Planning and Execution',
'Index Analysis',
'InList Rewrite disqualifed by MultiTable Expression',
'InList Rewrite Threshold Exceeded',
'InList Rewrite to Outer Join',
'InList Rewrite to Subquery',
'Multisource',
'Fast Path Function',
'Number Data Type',
'Autoreparse',
'Structure Data Type',
'Multiple Count Distinct',
'Not System-Default Map',
'Partial Redistribution Partial Duplication',
'Push TOP N into UNION ALL Derived Tables',
'Single Sender Row Re-Distribution',
'Soft RetLimit for number of rows AMP step returns',
'SET TRANSFORM',
'Statement Info Parcel',
'Two Maps')
-- Adding Missing Features
UNION
SEL 'Teradata ML Engine',NULL FROM DBC.DBCInfo
UNION
SEL 'Teradata Graph Engine',NULL FROM DBC.DBCInfo
UNION
SEL 'Teradata Query Service',NULL FROM DBC.DBCInfo
UNION
SEL 'Scoring Functions',NULL FROM DBC.DBCInfo
UNION
SEL 'nPath Function',NULL FROM DBC.DBCInfo
UNION
SEL 'Sessionization Function',NULL FROM DBC.DBCInfo
UNION
SEL 'Attribution Function',NULL FROM DBC.DBCInfo
UNION
SEL 'R tdplyr',NULL FROM DBC.DBCInfo
UNION
SEL 'Python teradataml',NULL FROM DBC.DBCInfo
UNION
SEL 'Text Analyzer Functions',NULL FROM DBC.DBCInfo
UNION
SEL 'Statistical Analysis Functions',NULL FROM DBC.DBCInfo
-- End of Adding Missing Features
) B
INNER JOIN DL_IDW_Analytics.ca_user_xref C
ON C.Username = A.Username
WHERE A.LogDate > DATE-30
GROUP BY 1,2,3;