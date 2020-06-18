
/*
--Query 24
Query Output File Name: Edge.csv
-- Generate Edge csv output for Gephi
-- Copy and paste to Edges.csv, remove first column (ie: 1,2,3..n)
-- Make sure you leave in the header row, and that numbers do not have commas or quotes around them
-- save as csv format
--Query Edge.csv
*/

/*{{save:Edge.csv}}*/
LOCKING ROW for ACCESS
SELECT    UPPER(TRIM(QTU1.DatabaseName) || '.' || TRIM(QTU1.TableName))  AS "Source"
        , UPPER(TRIM(QTU2.DatabaseName) || '.' || TRIM(QTU2.TableName))  AS "Target"
        , COUNT(DISTINCT QTU1.QueryID) AS "Weight"
		, 'Undirected' AS "Type"

FROM    (
                    SELECT   objectdatabasename AS DatabaseName
                           , ObjectTableName AS TableName
                           , QueryId
                    --FROM DBC.DBQLObjTbl   uncomment for DBC */
					 FROM PDCRINFO.DBQLObjTbl_Hst  /* uncomment for PDCR */
					WHERE Objecttype = 'Tab'
					AND ObjectTableName IS NOT NULL
                    AND ObjectColumnName IS NULL
					-- AND CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01'  uncomment for DBC */
					-- AND LogDate BETWEEN '2017-01-01' AND '2017-08-01'  uncomment for PDCR */
						AND LogDate BETWEEN {startdate} and {enddate}
						-- BETWEEN current_date - 90 AND current_date - 1  uncomment for PDCR */
					GROUP BY 1,2,3
                ) AS QTU1
                INNER JOIN
                (
                    SELECT   objectdatabasename AS DatabaseName
                           , ObjectTableName AS TableName
                           , QueryId
                    --FROM DBC.DBQLObjTbl  uncomment for DBC */
					 FROM PDCRINFO.DBQLObjTbl_Hst  /* uncomment for PDCR */
					WHERE Objecttype = 'Tab'
					AND ObjectTableName IS NOT NULL
                    AND ObjectColumnName IS NULL
					--AND CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01'  uncomment for DBC */
					-- AND LogDate BETWEEN '2017-01-01' AND '2017-08-01'  uncomment for PDCR */
						AND LogDate BETWEEN {startdate} and {enddate}
						-- TODO: BETWEEN current_date - 90 AND current_date - 1    uncomment for PDCR */
					GROUP BY 1,2,3
                ) AS QTU2
                ON QTU1.QueryID=QTU2.QueryID
				--INNER JOIN DBC.DBQLogTbl QU     uncomment for DBC */
                 INNER JOIN PDCRINFO.DBQLogTbl_Hst QU /* uncomment for PDCR */
                ON QTU1.QueryID=QU.QueryID
WHERE "Source" > "Target"  -- this ensures an edge only has one set, (a,b) never (a,b and b,a)
AND (QU.AMPCPUTime + QU.ParserCPUTime) > 0 -- FILTER: Only show affinity for resource consuming queries. Remove if you wish to see all.
--AND QTU1.DatabaseName NOT IN ('database_name', 'another_database_name')   FILTER: change to remove databases like GCFR

GROUP BY 1,2,4
;
