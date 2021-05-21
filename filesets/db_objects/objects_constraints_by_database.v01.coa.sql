/* Contraint analysis
Details and counts by DB
*/


CREATE MULTISET VOLATILE TABLE constraint_details
    (
     DatabaseName   VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
     TableName      VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
     ConstraintType VARCHAR(30)  CHARACTER SET UNICODE NOT CASESPECIFIC,
     Details        VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC)
NO PRIMARY INDEX
ON COMMIT PRESERVE ROWS;



INSERT INTO constraint_details
        SELECT
              DatabaseName,
              TableName,
              CASE WHEN IndexType IN ('U','P','Q') THEN 'Unique'
                   WHEN IndexType IN ('K') THEN 'Primary Key'
              END (VARCHAR(30)) AS ConstraintType,
              TRIM(TRAILING ','
                   FROM XMLAGG(ColumnName || ','
                   ORDER BY ColumnPosition)(VARCHAR(255))) AS Details
        FROM  DBC.IndicesV
        WHERE IndexType IN ('K','U','P','Q')
          AND UniqueFlag = 'Y'
        GROUP BY  DatabaseName, TableName, IndexType, IndexNumber

        UNION ALL

        SELECT  ChildDB,
                ChildTable,
                'Foreign Key',
                '>- ' || ParentDB || '.' || ParentTable
        FROM    DBC.RI_Distinct_ChildrenV

        UNION ALL

        SELECT  DatabaseName,
                TableName,
                'Column Constraint',
                REGEXP_SUBSTR(ColumnConstraint,'\(.*',1,1)
        FROM    DBC.ColumnsV
        WHERE   ColumnConstraint IS NOT NULL

        UNION ALL

        SELECT  DatabaseName,
                TableName,
                'Table Constraint',
                REGEXP_SUBSTR(ConstraintText,'\(.*',1,1)
        FROM    DBC.Table_LevelConstraintsV

        UNION ALL

        SELECT  COL.DatabaseName,
                COL.TableName,
                'Default',
                COL.ColumnName || ' = ' || COL.DefaultValue
        FROM    DBC.ColumnsV COL
        JOIN    DBC.Tablesv TAB
        ON      TAB.DatabaseName = COL.DatabaseName
        AND     TAB.TableName = COL.TableName
        AND     TAB.TableKind = 'T'
        WHERE   COL.DefaultValue IS NOT NULL
;

/*{{save:dat_dbobject_constraint_detail.csv}}*/
Select * from constraint_details;



create volatile table constraint_type_by_database as
(
SELECT
   ctt.ConstraintType
  ,COALESCE(ctc.DatabaseName, '') AS DatabaseName
  ,ZEROIFNULL(ctc.ConstraintCount) AS ConstraintCount
FROM
(
  SELECT  cast('Unique' as varchar(30)) AS ConstraintType FROM (sel 1 one) i1 UNION ALL
  SELECT 'Primary Key' FROM (sel 1 one) i2 UNION ALL
  SELECT 'Foreign Key' FROM (sel 1 one) i3 UNION ALL
  SELECT 'Column Constraint' FROM (sel 1 one) i4 UNION ALL
  SELECT 'Table Constraint' FROM (sel 1 one) i5
) ctt
LEFT JOIN
(
  SELECT
     DatabaseName
    ,ConstraintType
    ,COUNT(*) AS ConstraintCount
  FROM constraint_details
  GROUP BY 1,2
) ctc
ON ctt.ConstraintType = ctc.ConstraintType
) with data no primary index on commit preserve rows
;


/*{{save:dat_dbobject_constraint_count_per_database.csv}}*/
select * from constraint_type_by_database;
