SELECT  *
FROM    (
        SELECT  DatabaseName,
                TableName,
                CASE WHEN IndexType IN ('U','P','Q')
                     THEN 'Unique'
                     WHEN IndexType IN ('K')
                     THEN 'Primary Key'
                     END (VARCHAR(30)) AS ConstraintType,
                TRIM(TRAILING ','
                    FROM XMLAGG(ColumnName || ','
                    ORDER BY ColumnPosition)(VARCHAR(255))) AS Details
        FROM    DBC.IndicesV
        WHERE UniqueFlag = 'Y' AND IndexType IN ('K','U','P','Q')
        GROUP BY    DatabaseName,
                    TableName,
                    IndexType,
                    IndexNumber

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
        WHERE     COL.DefaultValue IS NOT NULL
        ) AS C
WHERE   DatabaseName NOT IN ('All', 'Crashdumps', 'DBC', 'dbcmngr',
        'Default', 'External_AP', 'EXTUSER', 'LockLogShredder', 'PUBLIC',
        'Sys_Calendar', 'SysAdmin', 'SYSBAR', 'SYSJDBC', 'SYSLIB',
        'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'TD_SERVER_DB', 'TDStats',
        'TD_SYSGPL', 'TD_SYSXML', 'TDMaps', 'TDPUSER', 'TDQCD',
        'tdwm', 'SQLJ', 'TD_SYSFNLIB', 'SYSSPATIAL')
ORDER BY    DatabaseName,
            TableName,
            ConstraintType;

			

SELECT
    'Object Type TCore Consumption' AS ReportName
    ,QryLog.LogDate
    ,EXTRACT(HOUR FROM QryLog.StartTime) AS LogHour
    ,ObjectDatabaseName
    ,ObjectTableName
    ,FT.ObjectTypeDesc AS FTObjectType
    ,SUM(QryLog.AMPCPUTime) AS CPUSecs
    ,CAST(CPUSecs / 10688.66 AS DECIMAL(9, 2)) AS TCoreH
FROM
    PDCRINFO.DBQLObjTbl OT
    LEFT JOIN
    FeatureObjectType FT
        ON OT.ObjectType = FT.ObjectType
    INNER JOIN
    PDCRINFO.DBQLogTbl QryLog
        ON QryLog.QueryID = OT.QueryID
            AND QryLog.LogDate = OT.LogDate
            AND QryLog.ProcID = OT.ProcID
WHERE
    OT.LogDate BETWEEN DATE -90 AND DATE -1

GROUP BY
    1
    ,2
    ,3
    ,4
    ,5
    ,6
ORDER BY
    QryLog.LogDate
    ,LogHour;
