/* dimension  for column types
*/
create volatile table column_types as (
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
      END as Column_Type,
CASE
    WHEN ColumnType = 'BF' THEN 'BYTE'
    WHEN ColumnType = 'BV' THEN 'VARBYTE'
    WHEN ColumnType = 'CF' THEN 'CHAR'
    WHEN ColumnType = 'CV' THEN 'VARCHAR'
    WHEN ColumnType = 'D ' THEN 'DECIMAL'
    WHEN ColumnType = 'DA' THEN 'DATE'
    WHEN ColumnType = 'F ' THEN 'FLOAT'
    WHEN ColumnType = 'I1' THEN 'BYTEINT'
    WHEN ColumnType = 'I2' THEN 'SMALLINT'
    WHEN ColumnType = 'I8' THEN 'BIGINT'
    WHEN ColumnType = 'I ' THEN 'INTEGER'
    WHEN ColumnType = 'AT' THEN 'TIME'
    WHEN ColumnType = 'TS' THEN 'TIMESTAMP'
    WHEN ColumnType = 'TZ' THEN 'TIME'
    WHEN ColumnType = 'SZ' THEN 'TIMESTAMP'
    WHEN ColumnType = 'YR' THEN 'INTERVAL'
    WHEN ColumnType = 'YM' THEN 'INTERVAL'
    WHEN ColumnType = 'MO' THEN 'INTERVAL'
    WHEN ColumnType = 'DY' THEN 'INTERVAL'
    WHEN ColumnType = 'DH' THEN 'INTERVAL'
    WHEN ColumnType = 'DM' THEN 'INTERVAL'
    WHEN ColumnType = 'DS' THEN 'INTERVAL'
    WHEN ColumnType = 'HR' THEN 'INTERVAL'
    WHEN ColumnType = 'HM' THEN 'INTERVAL'
    WHEN ColumnType = 'HS' THEN 'INTERVAL'
    WHEN ColumnType = 'MI' THEN 'INTERVAL'
    WHEN ColumnType = 'MS' THEN 'INTERVAL'
    WHEN ColumnType = 'SC' THEN 'INTERVAL'
    WHEN ColumnType = 'BO' THEN 'BLOB'
    WHEN ColumnType = 'CO' THEN 'CLOB'
    WHEN ColumnType = 'PD' THEN 'PERIOD'
    WHEN ColumnType = 'PM' THEN 'PERIOD'
    WHEN ColumnType = 'PS' THEN 'PERIOD'
    WHEN ColumnType = 'PT' THEN 'PERIOD'
    WHEN ColumnType = 'PZ' THEN 'PERIOD'
    WHEN ColumnType = 'UT' THEN COALESCE(ColumnUDTName,  '<Unknown> ' || ColumnType)
    WHEN ColumnType = '++' THEN 'TD_ANYTYPE'
    WHEN ColumnType = 'N'  THEN 'NUMBER'
    WHEN ColumnType = 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
    WHEN ColumnType = 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
    WHEN ColumnType = 'JN' AND CharType =1 AND cast(ColumnLength as INT)<64000 THEN 'JSON(Small)'
    WHEN ColumnType = 'JN' AND CharType<>1 AND cast(ColumnLength as INT)<32000 THEN 'JSON(Small)'
    WHEN ColumnType = 'JN' THEN 'JSON(Large)'
    WHEN ColumnType = 'VA' THEN 'TD_VALIST'
    WHEN ColumnType = 'XM' THEN 'XML'
    ELSE '<Unknown> ' || ColumnType
  END as Column_Category
  ,count(*) as Total_Cnt
FROM dbc.ColumnsV
WHERE ColumnType IS NOT NULL
group by 1,2
)with data no primary index on commit preserve rows;


/*{{save:dat_dbobject_count_per_columntype.csv}}*/
Select 
     '{siteid}' as Site_ID
    ,ctyp.* 
from column_types AS ctyp;