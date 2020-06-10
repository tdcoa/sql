-- Generate Node csv output for Gephi
-- Copy and paste to Nodes.csv, remove first column (ie: 1,2,3..n)
-- Make sure you leave in the header row, and that numbers do not have commas or quotes around them
-- save as csv format
LOCKING ROW for ACCESS
SELECT UPPER(SUM_ALLOC.DatabaseName||'.'||SUM_ALLOC.TableName) AS ID
        , UPPER(SUM_ALLOC.TableName) AS Label
		, UPPER(SUM_ALLOC.DatabaseName) AS Node_Database_Name
		, SUM_ALLOC.TableName AS Node_Table_Name
        , SUM_CountOfUses AS USE_Count
        , SUM_CountOfQuery AS QUERY_Count
        , CAST(SUM_AllocCPU AS DECIMAL(18,2)) AS CPU_Sec
        , CAST(SUM_AllocIO AS DECIMAL(18,2)) AS IO_GB
        , CPURank AS CPU_Rank
        , IORank AS IO_Rank
        , CAST(SUM_AllocCPU/NULLIFZERO(SUM_AllCPU)*100 AS DECIMAL(18,8)) AS  CPU_Ratio
        , CAST(SUM_AllocIO/NULLIFZERO(SUM_AllIO)*100 AS DECIMAL(18,6)) AS IO_Ratio
        , COALESCE(CAST(SUM(CurrentPerm) AS DECIMAL(18,2)), 0) / (1024 * 1024 * 1024) AS TableSize_GB
        , RANK() OVER(ORDER BY TableSize_GB DESC) AS TableSize_Rank
FROM (
        SELECT
          ALLOC.DatabaseName
        , ALLOC.TableName
        , SUM(1) AS SUM_CountOfQuery
        , SUM(ALLOC.CountOfUses) AS SUM_CountOfUses
        , SUM(ALLOC.Alloc_TableCPU) AS SUM_AllocCPU
        , SUM(ALLOC.Alloc_TableIO) AS SUM_AllocIO
        , RANK() OVER(ORDER BY SUM_AllocCPU DESC) AS CPURank
        , RANK() OVER(ORDER BY SUM_AllocIO DESC) AS IORank
       
        FROM (
       
                SELECT QTU1.DatabaseName
                , QTU1.TableName
                , QTU1.QueryID
                , QTU1.CountOfUses
                , QTU2.CountOfAllTableUses
                , CAST(QTU1.CountOfUses AS FLOAT)/QTU2.CountOfAllTableUses AS Alloc_Index
                , Alloc_Index * (QU.AMPCPUTime + QU.ParserCPUTime) AS Alloc_TableCPU
                , Alloc_Index * (COALESCE(spma.AvgIOPerReqGB, 1.0 / (1024 * 1024 * 1024))*QU.TotalIOCount) (FLOAT) AS Alloc_TableIO
                FROM
                (
                    SELECT   objectdatabasename AS DatabaseName
                           , ObjectTableName AS TableName
                           , QueryId
                           , SUM(FreqOfUse) AS CountOfUses
                    FROM DBC.DBQLObjTbl /* uncomment for DBC */
					-- FROM PDCRINFO.DBQLObjTbl  /* uncomment for PDCR */
					WHERE Objecttype = 'Tab'
					AND ObjectTableName IS NOT NULL
                    AND ObjectColumnName IS NULL
					AND CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for DBC */
					-- AND LogDate BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for PDCR */
					GROUP BY 1,2,3
                ) AS QTU1
                INNER JOIN
                (
                    SELECT   QueryID
                           , SUM(FreqOfUse) AS CountOfAllTableUses
                    FROM DBC.DBQLObjTbl /* uncomment for DBC */
					-- FROM PDCRINFO.DBQLObjTbl  /* uncomment for PDCR */
					WHERE Objecttype = 'Tab'
					AND ObjectTableName IS NOT NULL
                    AND ObjectColumnName IS NULL
					AND CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for DBC */
					-- AND LogDate BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for PDCR */
                    GROUP BY 1
                ) AS QTU2
                ON QTU1.QueryID=QTU2.QueryID
               
                INNER JOIN DBC.DBQLogTbl QU /* uncomment for DBC */
                -- INNER JOIN PDCRINFO.DBQLogTbl QU /* uncomment for PDCR */
                ON QTU1.QueryID=QU.QueryID
				
				LEFT OUTER JOIN (
				    SELECT    thedate
							, (sum((LogicalDeviceReadKB + LogicalDeviceWriteKB) / (1024 * 1024) ) (FLOAT))/ sum(FileAcqs +FilePreReads +MemTextPageReads +MemCtxtPageWrites+MemCtxtPageReads+FileWrites ) AS AvgIOPerReqGB
				    FROM DBC.ResSpmaView
				    WHERE  vproc1 > 0
				    GROUP BY thedate
                ) AS spma
                ON QU.CollectTimeStamp (DATE) = spma.thedate
 
        ) AS ALLOC
 
        GROUP BY 1,2
 
    ) AS SUM_ALLOC
 
LEFT JOIN
    (
    SELECT
            SUM(QU.AMPCPUTime + QU.ParserCPUTime) AS SUM_AllCPU
          , SUM(COALESCE(spma.AvgIOPerReqGB, 1.0 / (1024 * 1024 * 1024))*QU.TotalIOCount) (FLOAT) AS SUM_AllIO

    FROM DBC.DBQLogTbl QU /* uncomment for DBC */
    -- FROM PDCRINFO.DBQLogTbl QU /* uncomment for PDCR */
		LEFT OUTER JOIN (
		SELECT    thedate
				, (sum((LogicalDeviceReadKB + LogicalDeviceWriteKB) / (1024 * 1024) ) (FLOAT))/ sum(FileAcqs +FilePreReads +MemTextPageReads +MemCtxtPageWrites+MemCtxtPageReads+FileWrites ) AS AvgIOPerReqGB
		FROM DBC.ResSpmaView
		WHERE  vproc1 > 0
		GROUP BY thedate
	    ) AS spma
	    ON QU.CollectTimeStamp (DATE) = spma.thedate
	WHERE CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for DBC */
	-- WHERE LogDate BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for PDCR */

    ) AS SumAll
    ON 1=1
 
LEFT JOIN DBC.AllSpaceV SPACE
        ON SUM_ALLOC.DatabaseName=SPACE.DatabaseName
        AND SUM_ALLOC.TableName=SPACE.TableName
WHERE CPU_Sec > 0  -- FILTER: Only show affinity for resource consuming queries. Remove if you wish to see all.
AND SUM_ALLOC.DatabaseName NOT IN ('database_name', 'another_database_name')  -- FILTER: change to remove databases like GCFR
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;


-- Generate Edge csv output for Gephi
-- Copy and paste to Edges.csv, remove first column (ie: 1,2,3..n)
-- Make sure you leave in the header row, and that numbers do not have commas or quotes around them
-- save as csv format

LOCKING ROW for ACCESS
SELECT    UPPER(TRIM(QTU1.DatabaseName) || '.' || TRIM(QTU1.TableName))  AS "Source"
        , UPPER(TRIM(QTU2.DatabaseName) || '.' || TRIM(QTU2.TableName))  AS "Target"
        , COUNT(DISTINCT QTU1.QueryID) AS "Weight"
		, 'Undirected' AS "Type"

FROM    (
                    SELECT   objectdatabasename AS DatabaseName
                           , ObjectTableName AS TableName
                           , QueryId
                    FROM DBC.DBQLObjTbl /* uncomment for DBC */
					-- FROM PDCRINFO.DBQLObjTbl  /* uncomment for PDCR */
					WHERE Objecttype = 'Tab'
					AND ObjectTableName IS NOT NULL
                    AND ObjectColumnName IS NULL
					AND CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for DBC */
					-- AND LogDate BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for PDCR */
					GROUP BY 1,2,3
                ) AS QTU1
                INNER JOIN
                (
                    SELECT   objectdatabasename AS DatabaseName
                           , ObjectTableName AS TableName
                           , QueryId
                    FROM DBC.DBQLObjTbl /* uncomment for DBC */
					-- FROM PDCRINFO.DBQLObjTbl  /* uncomment for PDCR */
					WHERE Objecttype = 'Tab'
					AND ObjectTableName IS NOT NULL
                    AND ObjectColumnName IS NULL
					AND CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for DBC */
					-- AND LogDate BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for PDCR */
					GROUP BY 1,2,3
                ) AS QTU2
                ON QTU1.QueryID=QTU2.QueryID
				INNER JOIN DBC.DBQLogTbl QU /* uncomment for DBC */
                -- INNER JOIN PDCRINFO.DBQLogTbl QU /* uncomment for PDCR */
                ON QTU1.QueryID=QU.QueryID
WHERE "Source" > "Target"  -- this ensures an edge only has one set, (a,b) never (a,b and b,a)
AND (QU.AMPCPUTime + QU.ParserCPUTime) > 0 -- FILTER: Only show affinity for resource consuming queries. Remove if you wish to see all.
AND QTU1.DatabaseName NOT IN ('database_name', 'another_database_name')  -- FILTER: change to remove databases like GCFR

GROUP BY 1,2,4
;