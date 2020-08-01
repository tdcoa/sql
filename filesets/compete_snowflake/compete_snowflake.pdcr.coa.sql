/* Find out SET vs MULTISET Tables in the system
  parameters:
  - iopernode_90pct = 1000  (from archie)
*/

/* LOAD DIMENSIONS INTO VOLATILE TABLES:
*/;
/*{{temp:dim_dbobject.csv}}*/;
/*{{temp:dim_datatype.csv}}*/;
/*{{temp:dim_tablekind.csv}}*/;
/*{{temp:dim_tdinternal_databases.csv}}*/;

/*{{save:dat_dbobject_usage_per_tablekind.csv}}*/
SELECT
    'Object Type Definitions' AS ReportName
    ,Table_Bucket
    ,TableKind_Desc
    ,TableKind_Desc as Object_Type
    ,COUNT(*) AS ObjectCount
FROM DBC.Tables as  t
JOIN "dim_tablekind.csv" as tk
  on t.TableKind = tk.TableKind
WHERE DatabaseName NOT IN
  (select dbname from "dim_tdinternal_databases.csv")
GROUP BY 1,2;


/*{{save:dat_dbobject_table_multiset.csv}}*/
select CASE CheckOpt WHEN  'Y' THEN 'MULTISET Tables'
WHEN 'N' THEN 'SET Tables'
ELSE 'Others' END AS Table_Type,
Count(*) as Total_Count
from DBC.Tables
GROUP BY 1;


/*{{save:dat_dbobject_usage_per_type.csv}}*/
SELECT
    'Object Types Used and Frequency' AS ReportName
    ,FT.ObjectType
    ,ObjectTypeDesc
    ,ZEROIFNULL(Frequency_of_Use) AS Frequency_of_Use
FROM
    "dim_dbobject.csv" FT
    LEFT OUTER JOIN
    (
        SELECT
            ObjectType
            ,SUM(CAST(FreqofUse AS BIGINT)) AS Frequency_of_Use
        FROM
            PDCRINFO.DBQLObjTbl OT
        WHERE
            LogDate BETWEEN DATE -90 AND DATE -1
            AND OT.ObjectDatabaseName NOT IN ('All', 'ARCUSERS', 'console', 'Crashdumps', 'DBC', 'DBCMANAGER', 'dbcmngr', 'Default', 'External_AP', 'LockLogShredder', 'PUBLIC', 'SECADMIN', 'SPOOL_RESERVE', 'SQLJ', 'SysAdmin', 'SYSBAR', 'SYSDBA', 'SYSJDBC', 'SYSLIB', 'SYSSPATIAL', 'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'Sys_Calendar', 'TDPUSER', 'TDQCD', 'TDStats', 'tdwm', 'TD_COD', 'TD_RECONFIG', 'TD_SERVER_DB', 'TD_SYSFNLIB', 'TD_SYSGPL', 'TD_SYSXML', 'USERSPACE', 'viewpoint', 'PDCRINFO', 'PDCRADM')
        GROUP BY
            1
    ) OT
        ON FT.ObjectType = OT.ObjectType
ORDER BY ObjectTypeDesc;


/*{{save:dat_dbobject_tcore_per_type.csv}}*/
SELECT
    'Object Type TCore Consumption' AS ReportName
    ,QryLog.LogDate
    ,EXTRACT(HOUR FROM QryLog.StartTime) AS LogHour
    ,ObjectDatabaseName
    ,ObjectTableName
    ,FT.ObjectTypeDesc AS FTObjectType
    ,SUM(QryLog.AMPCPUTime) AS CPUSecs
    ,CAST(CPUSecs / nullifzero(1000) AS DECIMAL(9, 2)) AS TCoreH
FROM
    PDCRINFO.DBQLObjTbl OT
    LEFT JOIN
    "dim_dbobject.csv" FT
        ON OT.ObjectType = FT.ObjectType
    INNER JOIN
    PDCRINFO.DBQLogTbl QryLog
        ON QryLog.QueryID = OT.QueryID
            AND QryLog.LogDate = OT.LogDate
            AND QryLog.ProcID = OT.ProcID
WHERE
    OT.LogDate BETWEEN DATE -90 AND DATE -1
    AND OT.ObjectType = 'TbO'
GROUP BY 1,2,3,4,5,6;


/*{{save:dat_dbobject_columns_and_indexes.csv}}*/
SELECT
    'Data Type Definitions' AS ReportName
    ,Cols.DatabaseName
    ,Cols.TableName
    ,Cols.ColumnName
    ,
    CASE
        WHEN Cols.ColumnType = '++'
            THEN 'TD_ANYTYPE'
        WHEN Cols.ColumnType = 'AN'
            THEN 'ARRAY (multidimensional)'
        WHEN Cols.ColumnType = 'A1'
            THEN 'ARRAY (one dimensional)'
        WHEN Cols.ColumnType = 'I8'
            THEN 'BIGINT'
        WHEN Cols.ColumnType = 'BO'
            THEN 'BINARY LARGE OBJECT'
        WHEN Cols.ColumnType = 'BF'
            THEN 'BYTE'
        WHEN Cols.ColumnType = 'BV'
            THEN 'BYTE VARYING'
        WHEN Cols.ColumnType = 'I1'
            THEN 'BYTEINT'
        WHEN Cols.ColumnType = 'CF'
            THEN 'CHARACTER (fixed)'
        WHEN Cols.ColumnType = 'CV'
            THEN 'CHARACTER (varying)'
        WHEN Cols.ColumnType = 'CO'
            THEN 'CHARACTER LARGE OBJECT'
        WHEN Cols.ColumnType = 'DA'
            THEN 'DATE'
        WHEN Cols.ColumnType = 'D'
            THEN 'DECIMAL'
        WHEN Cols.ColumnType = 'F'
            THEN 'FLOAT'
        WHEN Cols.ColumnType = 'I'
            THEN 'INTEGER'
        WHEN Cols.ColumnType = 'DY'
            THEN 'INTERVAL DAY'
        WHEN Cols.ColumnType = 'DH'
            THEN 'INTERVAL DAY TO HOUR'
        WHEN Cols.ColumnType = 'DM'
            THEN 'INTERVAL DAY TO MINUTE'
        WHEN Cols.ColumnType = 'DS'
            THEN 'INTERVAL DAY TO SECOND'
        WHEN Cols.ColumnType = 'HR'
            THEN 'INTERVAL HOUR'
        WHEN Cols.ColumnType = 'HM'
            THEN 'INTERVAL HOUR TO MINUTE'
        WHEN Cols.ColumnType = 'HS'
            THEN 'INTERVAL HOUR TO SECOND'
        WHEN Cols.ColumnType = 'MI'
            THEN 'INTERVAL MINUTE'
        WHEN Cols.ColumnType = 'MS'
            THEN 'INTERVAL MINUTE TO SECOND'
        WHEN Cols.ColumnType = 'MO'
            THEN 'INTERVAL MONTH'
        WHEN Cols.ColumnType = 'SC'
            THEN 'INTERVAL SECOND'
        WHEN Cols.ColumnType = 'YR'
            THEN 'INTERVAL YEAR'
        WHEN Cols.ColumnType = 'YM'
            THEN 'INTERVAL YEAR TO MONTH'
        WHEN Cols.ColumnType = 'JN'
            THEN
                CASE
                    WHEN Cols.StorageFormat = 'TEXT'
                        THEN 'JSON'
                    ELSE
                        Cols.StorageFormat
                END
        WHEN Cols.ColumnType = 'N'
            THEN 'NUMBER'
        WHEN Cols.ColumnType = 'PD'
            THEN 'PERIOD(DATE)'
        WHEN Cols.ColumnType = 'PZ'
            THEN 'PERIOD(TIME(n) WITH TIME ZONE)'
        WHEN Cols.ColumnType = 'PT'
            THEN 'PERIOD(TIME(n))'
        WHEN Cols.ColumnType = 'PM'
            THEN 'PERIOD(TIMESTAMP(n) WITH TIME ZONE)'
        WHEN Cols.ColumnType = 'PS'
            THEN 'PERIOD(TIMESTAMP(n))'
        WHEN Cols.ColumnType = 'I2'
            THEN 'SMALLINT'
        WHEN Cols.ColumnType = 'AT'
            THEN 'TIME'
        WHEN Cols.ColumnType = 'TZ'
            THEN 'TIME WITH TIME ZONE'
        WHEN Cols.ColumnType = 'TS'
            THEN 'TIMESTAMP'
        WHEN Cols.ColumnType = 'SZ'
            THEN 'TIMESTAMP WITH TIME ZONE'
        WHEN Cols.ColumnType = 'UT'
            THEN 'USER-DEFINED TYPE (all types)'
        WHEN Cols.ColumnType = 'XM'
            THEN 'XML'
        ELSE
            Cols.ColumnType
    END AS ColumnDataType
    ,DecimalTotalDigits AS DecNum
    ,DecimalFractionalDigits AS DecScale
    ,Cols.ColumnLength
    ,TRIM(Cols.ColumnFormat) AS ColumnFormat
    ,
    CASE StorageFormat
        WHEN 'TEXT'
            THEN 'JSON'
        ELSE
            StorageFormat
    END AS StorageFormat
    ,
    CASE Nullable
        WHEN 'Y'
            THEN ' '
        WHEN 'N'
            THEN 'NOT NULL'
    END AS NullConstraint
    ,
    CASE
        WHEN Inds.IndexType = 'P'
            AND Inds.UniqueFlag = 'Y'
            THEN 'Unique Primary Index (UPI)'
        WHEN Inds.IndexType = 'P'
            AND Inds.UniqueFlag = 'N'
            THEN 'Non-Unique Primary Index (NUPI)'
        WHEN Inds.IndexType = 'Q'
            THEN 'Partitioned Primary Index'
        WHEN Inds.IndexType = 'S'
            AND Inds.UniqueFlag = 'Y'
            THEN 'Unique Secondary Index (USI)'
        WHEN Inds.IndexType = 'S'
            AND Inds.UniqueFlag = 'N'
            THEN 'Non-Unique Secondary Index (NUSI)'
        WHEN Inds.IndexType = 'U'
            THEN 'Unique Secondary with NOT NULL'
        WHEN Inds.IndexType = 'K'
            THEN 'Primary Key'
        WHEN Inds.IndexType = 'J'
            THEN 'Join Index'
        WHEN Inds.IndexType = 'V'
            THEN 'Value Ordered Secondary Index'
        WHEN Inds.IndexType = 'H'
            THEN 'Hash Ordered ALL (covering) Secondary Index'
        WHEN Inds.IndexType = 'O'
            THEN 'Value Ordered ALL (covering) Secondary Index'
        WHEN Inds.IndexType = 'I'
            THEN 'Ordering Column of a Composite Secondary Index'
        WHEN Inds.IndexType = 'M'
            THEN 'Multi-Column Statistics'
        WHEN Inds.IndexType = 'D'
            THEN 'Derived Column Partition Statistics'
        WHEN Inds.IndexType IS NULL
            THEN ' '
        ELSE
            Inds.IndexType
    END AS IndexTypeDesc
FROM
    DBC.ColumnsV Cols
    LEFT JOIN
    DBC.Tables Tbls
        ON Tbls.DatabaseName = Cols.DatabaseName
            AND Tbls.TableName = Cols.TableName
    LEFT JOIN
    DBC.ColumnStatsV ColStats
        ON ColStats.DatabaseName = Cols.DatabaseName
            AND ColStats.TableName = Cols.TableName
            AND ColStats.ColumnName = Cols.ColumnName
    LEFT JOIN
    DBC.Indices Inds
        ON Inds.DatabaseName = Cols.DatabaseName
            AND Inds.TableName = Cols.TableName
            AND Inds.ColumnName = Cols.ColumnName
WHERE
    Cols.DatabaseName NOT IN ('All', 'ARCUSERS', 'console', 'Crashdumps', 'DBC', 'DBCMANAGER', 'dbcmngr', 'Default', 'External_AP', 'LockLogShredder', 'PUBLIC', 'SECADMIN', 'SPOOL_RESERVE', 'SQLJ', 'SysAdmin', 'SYSBAR', 'SYSDBA', 'SYSJDBC', 'SYSLIB', 'SYSSPATIAL', 'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'Sys_Calendar', 'TDPUSER', 'TDQCD', 'TDStats', 'tdwm', 'TD_COD', 'TD_RECONFIG', 'TD_SERVER_DB', 'TD_SYSFNLIB', 'TD_SYSGPL', 'TD_SYSXML', 'USERSPACE', 'viewpoint')
    AND TableKind <> 'V'
    AND Cols.ColumnType = 'JN' ;


/*{{save:dat_dbobject_count_per_datatype.csv}}*/
SELECT
    'Data Type Usage' AS ReportName
    ,DataTypeDesc
    ,ZEROIFNULL(DataTypeCount) AS DataTypeCount
FROM
    "dim_datatype.csv" DT
    LEFT JOIN
    (
        SELECT
            CASE
                WHEN ColumnType = 'JN'
                    AND StorageFormat = 'TEXT'
                    THEN 'JN'
                WHEN ColumnType = 'JN'
                    AND StorageFormat = 'BSON'
                    THEN 'BN'
                WHEN ColumnType = 'JN'
                    AND StorageFormat = 'UBJSON'
                    THEN 'UN'
                ELSE
                    ColumnType
            END AS ColumnType
            ,COUNT(*) AS DataTypeCount
        FROM
            DBC.ColumnsV
        WHERE
            DatabaseName NOT IN ('All', 'ARCUSERS', 'console', 'Crashdumps', 'DBC', 'DBCMANAGER', 'dbcmngr', 'Default', 'External_AP', 'LockLogShredder', 'PUBLIC', 'SECADMIN', 'SPOOL_RESERVE', 'SQLJ', 'SysAdmin', 'SYSBAR', 'SYSDBA', 'SYSJDBC', 'SYSLIB', 'SYSSPATIAL', 'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'Sys_Calendar', 'TDPUSER', 'TDQCD', 'TDStats', 'tdwm', 'TD_COD', 'TD_RECONFIG', 'TD_SERVER_DB', 'TD_SYSFNLIB', 'TD_SYSGPL', 'TD_SYSXML', 'USERSPACE', 'viewpoint')
        GROUP BY
            1
    ) AS Col
        ON DT.DataType = Col.ColumnType ;


/*{{save:dat_dbobject_indexing_and_PPI.csv}}*/
SELECT
    'Index Types and Partitioning' AS ReportName
    ,DT.DataTypeDesc AS ColumnDataType
    ,COUNT(*) AS DataTypeCount
FROM
    DBC.ColumnsV Cols
    LEFT JOIN
    DBC.Tables Tbls
        ON Tbls.DatabaseName = Cols.DatabaseName
            AND Tbls.TableName = Cols.TableName
    LEFT JOIN
    "dim_datatype.csv" DT
        ON DT.DataType = Cols.ColumnType
    LEFT JOIN
    DBC.ColumnStatsV ColStats
        ON ColStats.DatabaseName = Cols.DatabaseName
            AND ColStats.TableName = Cols.TableName
            AND ColStats.ColumnName = Cols.ColumnName
    LEFT JOIN
    DBC.Indices Inds
        ON Inds.DatabaseName = Cols.DatabaseName
            AND Inds.TableName = Cols.TableName
            AND Inds.ColumnName = Cols.ColumnName
WHERE
    TableKind <> 'V'
    AND ColumnDataType IS NOT NULL
GROUP BY 1,2;

/*{{save:dat_dbobject_count_per_statementtype.csv}}*/
SELECT
    'SQL Statement Type Usage' AS ReportName
    ,StatementType
    ,COUNT(*) AS Frequency_Count
FROM
    PDCRINFO.DBQLogTbl
WHERE
    LogDate BETWEEN DATE -8 AND DATE -1
GROUP BY 1,2;


/*{{save:dat_dbobject_count_per_columntype.csv}}*/
SELECT
CASE ColumnType
    WHEN 'BF' THEN 'BYTE('            || TRIM(ColumnLength (FORMAT '-(9)9')) || ')'
    WHEN 'BV' THEN 'VARBYTE('         || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
    WHEN 'CF' THEN 'CHAR('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
    WHEN 'CV' THEN 'VARCHAR('         || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
    WHEN 'D ' THEN 'DECIMAL('         || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ','
                                      || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'DA' THEN 'DATE'
    WHEN 'F ' THEN 'FLOAT'
    WHEN 'I1' THEN 'BYTEINT'
    WHEN 'I2' THEN 'SMALLINT'
    WHEN 'I8' THEN 'BIGINT'
    WHEN 'I ' THEN 'INTEGER'
    WHEN 'AT' THEN 'TIME('            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'TS' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'TZ' THEN 'TIME('            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'
    WHEN 'SZ' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'
    WHEN 'YR' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'YM' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MONTH'
    WHEN 'MO' THEN 'INTERVAL MONTH('  || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'DY' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'DH' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO HOUR'
    WHEN 'DM' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MINUTE'
    WHEN 'DS' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                      || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'HR' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'HM' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MINUTE'
    WHEN 'HS' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                      || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'MI' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'MS' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                      || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'SC' THEN 'INTERVAL SECOND(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ','
                                      || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
    WHEN 'BO' THEN 'BLOB('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
    WHEN 'CO' THEN 'CLOB('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'

    WHEN 'PD' THEN 'PERIOD(DATE)'
    WHEN 'PM' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE)'
    WHEN 'PS' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
    WHEN 'PT' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
    WHEN 'PZ' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE)'
    WHEN 'UT' THEN COALESCE(ColumnUDTName,  '<Unknown> ' || ColumnType)

    WHEN '++' THEN 'TD_ANYTYPE'
    WHEN 'N'  THEN 'NUMBER('          || CASE WHEN DecimalTotalDigits = -128 THEN '*' ELSE TRIM(DecimalTotalDigits (FORMAT '-(9)9')) END
                                      || CASE WHEN DecimalFractionalDigits IN (0, -128) THEN '' ELSE ',' || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) END
                                      || ')'
    WHEN 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
    WHEN 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)

    WHEN 'JN' THEN 'JSON('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
    WHEN 'VA' THEN 'TD_VALIST'
    WHEN 'XM' THEN 'XML'

    ELSE '<Unknown> ' || ColumnType
  END
  || CASE
        WHEN ColumnType IN ('CV', 'CF', 'CO')
        THEN CASE CharType
                WHEN 1 THEN ' CHARACTER SET LATIN'
                WHEN 2 THEN ' CHARACTER SET UNICODE'
                WHEN 3 THEN ' CHARACTER SET KANJISJIS'
                WHEN 4 THEN ' CHARACTER SET GRAPHIC'
                WHEN 5 THEN ' CHARACTER SET KANJI1'
                ELSE ''
             END
         ELSE ''
      END as C_TYPE,

CASE ColumnType
    WHEN 'BF' THEN 'BYTE'
    WHEN 'BV' THEN 'VARBYTE'
    WHEN 'CF' THEN 'CHAR'
    WHEN 'CV' THEN 'VARCHAR'
    WHEN 'D ' THEN 'DECIMAL'
    WHEN 'DA' THEN 'DATE'
    WHEN 'F ' THEN 'FLOAT'
    WHEN 'I1' THEN 'BYTEINT'
    WHEN 'I2' THEN 'SMALLINT'
    WHEN 'I8' THEN 'BIGINT'
    WHEN 'I ' THEN 'INTEGER'
    WHEN 'AT' THEN 'TIME'
    WHEN 'TS' THEN 'TIMESTAMP'
    WHEN 'TZ' THEN 'TIME'
    WHEN 'SZ' THEN 'TIMESTAMP'
    WHEN 'YR' THEN 'INTERVAL'
    WHEN 'YM' THEN 'INTERVAL'
    WHEN 'MO' THEN 'INTERVAL'
    WHEN 'DY' THEN 'INTERVAL'
    WHEN 'DH' THEN 'INTERVAL'
    WHEN 'DM' THEN 'INTERVAL'
    WHEN 'DS' THEN 'INTERVAL'
    WHEN 'HR' THEN 'INTERVAL'
    WHEN 'HM' THEN 'INTERVAL'
    WHEN 'HS' THEN 'INTERVAL'
    WHEN 'MI' THEN 'INTERVAL'
    WHEN 'MS' THEN 'INTERVAL'
    WHEN 'SC' THEN 'INTERVAL'
    WHEN 'BO' THEN 'BLOB'
    WHEN 'CO' THEN 'CLOB'

    WHEN 'PD' THEN 'PERIOD'
    WHEN 'PM' THEN 'PERIOD'
    WHEN 'PS' THEN 'PERIOD'
    WHEN 'PT' THEN 'PERIOD'
    WHEN 'PZ' THEN 'PERIOD'
    WHEN 'UT' THEN COALESCE(ColumnUDTName,  '<Unknown> ' || ColumnType)

    WHEN '++' THEN 'TD_ANYTYPE'
    WHEN 'N'  THEN 'NUMBER'
    WHEN 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
    WHEN 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)

    WHEN 'JN' THEN 'JSON('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
    WHEN 'VA' THEN 'TD_VALIST'
    WHEN 'XM' THEN 'XML'

    ELSE '<Unknown> ' || ColumnType
  END
  as C_CAT,
count(*) as Total
FROM dbc.ColumnsV
WHERE ColumnType IS NOT NULL
group by 1,2
;

/*{{save:dat_dbobject_count_per_columnformats.csv}}*/
SELECT
CASE WHEN ColumnFormat IS NOT NULL
THEN 'FORMATTED' ELSE 'NO DEFAULT FORMAT'
END AS COLUMN_FORMAT, count(*) from DBC.COlumnsV group by 1;
