REPLACE VIEW DL_IDW_Analytics.UserFeatureUsage AS
LOCK ROW FOR ACCESS

SELECT C.SubDepartment,
	   C.Department,
		CASE WHEN B.FEATURENAME LIKE '%Partition%' THEN 'Partitioning'
	   		WHEN B.FEATURENAME LIKE '%DBQL%' THEN 'Logging'
			WHEN B.FEATURENAME LIKE '%LOB%' THEN 'LOB'
			WHEN B.FEATURENAME LIKE '%Geospatial%' THEN 'Geospatial'
			WHEN B.FEATURENAME LIKE '%QueryGrid%' THEN 'QueryGrid'
			ELSE B.FEATURENAME END AS FEATURENAME,
	   SUM(GETBIT(A.FEATUREUSAGE,(2047 - B.FEATUREBITPOS))) AS FEATUREUSECOUNT 
FROM PDCRINFO.DbqlogTbl A
CROSS JOIN DBC.QRYLOGFEATURELISTV B
INNER JOIN DL_IDW_Analytics.ca_user_xref C
ON C.Username = A.Username
WHERE A.LogDate > DATE-30
GROUP BY 1,2,3
HAVING FEATUREUSECOUNT IS NOT NULL




select *
from
(
SELECT C.SubDepartment,
	   C.Department,
	   B.FEATURENAME, 
	   GETBIT(A.FEATUREUSAGE,(2047 - B.FEATUREBITPOS)) AS FEATUREUSECOUNT 
FROM PDCRINFO.DbqlogTbl A
CROSS JOIN DBC.QRYLOGFEATURELISTV B
INNER JOIN DL_IDW_Analytics.ca_user_xref C
ON C.Username = A.Username
AND A.LogDate > DATE-30
AND FEATUREUSECOUNT IS NOT NULL
) dt

PIVOT (SUM(FEATUREUSECOUNT) FOR FEATURENAME IN
(
'2 Byte Partitioning' AS "2 Byte Partitioning",
'3D Geospatial' AS "3D Geospatial",
'8 Byte Partitioning' AS "8 Byte Partitioning",
'Aggregation Push for Union All' AS "Aggregation Push for Union All",
'Analytics Extensions' AS "Analytics Extensions",
'Archive/Restore' AS "Archive/Restore",
'Array Data Type' AS "Array Data Type",
'Auto Compression' AS "Auto Compression",
'Autoreparse' AS "Autoreparse",
'Binary Join Push for Union All' AS "Binary Join Push for Union All",
'Binary LOB' AS "Binary LOB",
'Bit Manipulation Function' AS "Bit Manipulation Function",
'Block Level Compression' AS "Block Level Compression",
'Character LOB' AS "Character LOB",
'Collect Statistics' AS "Collect Statistics",
'Column Partitioning and Row Partitioning' AS "Column Partitioning and Row Partitioning",
'Column-Partitioned and a NoPI' AS "Column-Partitioned and a NoPI",
'Column-Partitioned and a PA' AS "Column-Partitioned and a PA",
'Column-Partitioned and a PI' AS "Column-Partitioned and a PI",
'Composite Secondary Index' AS "Composite Secondary Index",
'Data Stream Architecture' AS "Data Stream Architecture",
'Dataset Data Type Storage Format CSV' AS "Dataset Data Type Storage Format CSV",
'Dataset Data Type' AS "Dataset Data Type",
'DBQL EXPLAIN Logging' AS "DBQL EXPLAIN Logging",
'DBQL OBJECT USECOUNT Logging' AS "DBQL OBJECT USECOUNT Logging",
'DBQL OBJECTS Logging' AS "DBQL OBJECTS Logging",
'DBQL PARAMINFO Logging' AS "DBQL PARAMINFO Logging",
'DBQL SQL Logging' AS "DBQL SQL Logging",
'DBQL STATSUSAGE Logging' AS "DBQL STATSUSAGE Logging",
'DBQL STEP Logging' AS "DBQL STEP Logging",
'DBQL THRESHOLD Enabled' AS "DBQL THRESHOLD Enabled",
'DBQL UTILITYINFO Logging' AS "DBQL UTILITYINFO Logging",
'DBQL XMLPLAN Logging' AS "DBQL XMLPLAN Logging",
'Distinct Data Type' AS "Distinct Data Type",
'Dot Notation' AS "Dot Notation",
'Dynamic Parameterization of Literals Feature' AS "Dynamic Parameterization of Literals Feature",
'External Stored Procedure' AS "External Stored Procedure",
'Fallback' AS "Fallback",
'Fast Path Function' AS "Fast Path Function",
'Fastexport Protocol' AS "Fastexport Protocol",
'Fastload Protocol' AS "Fastload Protocol",
'Foreign Function' AS "Foreign Function",
'Function Alias' AS "Function Alias",
'Geospatial Index Distance Join Predicate' AS "Geospatial Index Distance Join Predicate",
'Geospatial Index Join Predicate' AS "Geospatial Index Join Predicate",
'Geospatial Index Single Table Distance Predicate' AS "Geospatial Index Single Table Distance Predicate",
'Geospatial Index Single Table Predicate' AS "Geospatial Index Single Table Predicate",
'Geospatial' AS "Geospatial",
'Hashed Table' AS "Hashed Table",
'In-Memory All Rows One AMP Optimization' AS "In-Memory All Rows One AMP Optimization",
'In-Memory AVX2 Support' AS "In-Memory AVX2 Support",
'In-Memory Bulk Qualification Enhancements' AS "In-Memory Bulk Qualification Enhancements",
'In-Memory Enhancement for Outer Join' AS "In-Memory Enhancement for Outer Join",
'In-Memory Enhancement for PRPD Join' AS "In-Memory Enhancement for PRPD Join",
'Incremental Planning and Execution' AS "Incremental Planning and Execution",
'Index Analysis' AS "Index Analysis",
'InList Rewrite disqualifed by MultiTable Expression' AS "InList Rewrite disqualifed by MultiTable Expression",
'InList Rewrite Threshold Exceeded' AS "InList Rewrite Threshold Exceeded",
'InList Rewrite to Outer Join' AS "InList Rewrite to Outer Join",
'InList Rewrite to Subquery' AS "InList Rewrite to Subquery",
'Join Index' AS "Join Index",
'JSON Data Type' AS "JSON Data Type",
'LEAD LAG Analytics' AS "LEAD LAG Analytics",
'Load Isolation' AS "Load Isolation",
'MloadX Protocol' AS "MloadX Protocol",
'Multi Level Partitioning' AS "Multi Level Partitioning",
'Multiload Protocol' AS "Multiload Protocol",
'Multiple Count Distinct' AS "Multiple Count Distinct",
'MULTISET Table' AS "MULTISET Table",
'Multisource' AS "Multisource",
'No Primary Index' AS "No Primary Index",
'Non Unique Hashed Index' AS "Non Unique Hashed Index",
'Non Unique UnHashed Index' AS "Non Unique UnHashed Index",
'Not System-Default Map' AS "Not System-Default Map",
'Number Data Type' AS "Number Data Type",
'ODBC Scalar Functions' AS "ODBC Scalar Functions",
'Parameterized Query' AS "Parameterized Query",
'Partial Redistribution Partial Duplication' AS "Partial Redistribution Partial Duplication",
'Partition Analysis' AS "Partition Analysis",
'Partition Level Locking' AS "Partition Level Locking",
'Period Data Type' AS "Period Data Type",
'Primary Index' AS "Primary Index",
'Push TOP N into UNION ALL Derived Tables' AS "Push TOP N into UNION ALL Derived Tables",
'Query Grid' AS "Query Grid",
'Queryable View Column Info' AS "Queryable View Column Info",
'Queryband' AS "Queryband",
'QueryGrid Common Remote Table Elimination' AS "QueryGrid Common Remote Table Elimination",
'QueryGrid Remote Tables' AS "QueryGrid Remote Tables",
'R Table Operator' AS "R Table Operator",
'Redrive' AS "Redrive",
'Replication' AS "Replication",
'Row Partitioning' AS "Row Partitioning",
'Scalar Sub Query' AS "Scalar Sub Query",
'Script Table Operator' AS "Script Table Operator",
'Secondary Index' AS "Secondary Index",
'SET Table' AS "SET Table",
'SET TRANSFORM' AS "SET TRANSFORM",
'Single Sender Row Re-Distribution' AS "Single Sender Row Re-Distribution",
'Small LOB' AS "Small LOB",
'Soft RetLimit for number of rows AMP step returns' AS "Soft RetLimit for number of rows AMP step returns",
'Sparse Map' AS "Sparse Map",
'Statement Info Parcel' AS "Statement Info Parcel",
'Structure Data Type' AS "Structure Data Type",
'Table Function' AS "Table Function",
'Table Operator' AS "Table Operator",
'Teradata Columnar' AS "Teradata Columnar",
'Teradata In-Memory' AS "Teradata In-Memory",
'Teradata Pivot' AS "Teradata Pivot",
'Teradata Remote Query' AS "Teradata Remote Query",
'Teradata Stored Procedure' AS "Teradata Stored Procedure",
'Teradata Temporal' AS "Teradata Temporal",
'Teradata Unity' AS "Teradata Unity",
'Teradata Unpivot' AS "Teradata Unpivot",
'Time Series Table' AS "Time Series Table",
'Trigger' AS "Trigger",
'Two Maps' AS "Two Maps",
'Union All Pushdown' AS "Union All Pushdown",
'Unique Hashed Index' AS "Unique Hashed Index",
'Unique UnHashed Index' AS "Unique UnHashed Index",
'User Defined Function' AS "User Defined Function",
'User Defined Type' AS "User Defined Type",
'XML Data Type' AS "XML Data Type"
)) TMP