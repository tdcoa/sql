/*EXEC SpaceByDomainSubject

The SpaceByDomainSubject macro generates CURRENTPERM consumption by domain and subject area.

parameters: none
 */

/*{{save:SpaceByDomainSubject.csv}}*/
SELECT
SUM(CurrentPerm)/1024/1024/1024 AS "CurrentPerm in GB"
,DataDomain
,SubjectArea
FROM DBC.TableSize D
INNER JOIN ca_table_xref T
   ON D.DatabaseName = T.DatabaseName
  AND D.TableName = T.TableName
GROUP BY 2,3;
