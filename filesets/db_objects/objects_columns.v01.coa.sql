/* column type analysis
  Parameters:
  - siteid:  {siteid}

  Dependencies:
  -  dim_object
*/
create volatile table column_types as
( SELECT
   COALESCE(DT.DataTypeDesc, 'Unknown - ' || C.ColumnType) AS DataType
  ,COALESCE(DT.ColumnCategory || C.Column_Type_Ext, DT.DataTypeDesc, 'Unknown - ' || C.ColumnType) AS ColumnType
  ,C.IdColType AS IdentityColumnType
  ,COALESCE(DT.ColumnCategory || C.Column_Category_Ext, DT.ColumnCategory , 'Unknown - ' || C.ColumnType) AS ColumnCategory
  ,C.FormatInd
  ,ZEROIFNULL(C.ColumnCount) AS ColumnCount
 FROM (
       SELECT
         ColumnType
        ,IdColType
        ,COALESCE(StorageFormat, '') AS StorageFormat
        ,CASE WHEN ColumnFormat IS NOT NULL THEN 'Y' ELSE 'N' END AS FormatInd
        ,CASE ColumnType
           WHEN 'BF' THEN '(' || TRIM(ColumnLength (FORMAT '-(9)9')) || ')'
           WHEN 'BV' THEN '(' || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
           WHEN 'CF' THEN '(' || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
           WHEN 'CV' THEN '(' || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
           WHEN 'D'  THEN '(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ','
                              || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'AT' THEN '(' || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'TS' THEN '(' || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'TZ' THEN '(' || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'
           WHEN 'SZ' THEN '(' || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'
           WHEN 'YR' THEN ' YEAR('  || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'YM' THEN ' YEAR('  || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')' || ' TO MONTH'
           WHEN 'MO' THEN ' MONTH(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'DY' THEN ' DAY('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'DH' THEN ' DAY('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')' || ' TO HOUR'
           WHEN 'DM' THEN ' DAY('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')' || ' TO MINUTE'
           WHEN 'DS' THEN ' DAY('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')' || ' TO SECOND('
                                    || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'HR' THEN ' HOUR('  || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'HM' THEN ' HOUR('  || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')' || ' TO MINUTE'
           WHEN 'HS' THEN ' HOUR('  || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')' || ' TO SECOND('
                                    || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'MI' THEN ' MINUTE('|| TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'MS' THEN ' MINUTE('|| TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
                                    || ' TO SECOND(' || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'SC' THEN ' SECOND('|| TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ','
                                    || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
           WHEN 'BO' THEN '(' || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
           WHEN 'CO' THEN '(' || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
           WHEN 'PD' THEN '(DATE)'
           WHEN 'PM' THEN '(TIMESTAMP('|| TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE)'
           WHEN 'PS' THEN '(TIMESTAMP('|| TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
           WHEN 'PT' THEN '(TIME('     || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
           WHEN 'PZ' THEN '(TIME('     || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE)'
           WHEN 'UT' THEN COALESCE(ColumnUDTName,  '<Unknown> ' || ColumnType)
           WHEN 'N'  THEN '(' || CASE WHEN DecimalTotalDigits = -128 THEN '*' ELSE TRIM(DecimalTotalDigits (FORMAT '-(9)9')) END
                              || CASE WHEN DecimalFractionalDigits IN (0, -128) THEN '' ELSE ',' || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) END
                              || ')'
          WHEN 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
          WHEN 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
          WHEN 'JN' THEN '(' || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
          ELSE ''
          END
          ||
          CASE
          WHEN ColumnType IN ('CV', 'CF', 'CO') THEN
              CASE CharType WHEN 1 THEN ' CHARACTER SET LATIN'
                            WHEN 2 THEN ' CHARACTER SET UNICODE'
                            WHEN 3 THEN ' CHARACTER SET KANJISJIS'
                            WHEN 4 THEN ' CHARACTER SET GRAPHIC'
                            WHEN 5 THEN ' CHARACTER SET KANJI1'
                            ELSE '' END
          ELSE ''
        END as Column_Type_Ext
        ,CASE WHEN ColumnType = 'UT' THEN COALESCE(ColumnUDTName, '<Unknown> ' || ColumnType)
              WHEN ColumnType = 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
              WHEN ColumnType = 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
              WHEN ColumnType = 'JN' AND CharType =1 AND cast(ColumnLength as INT)<64000 THEN '(Small)'
              WHEN ColumnType = 'JN' AND CharType<>1 AND cast(ColumnLength as INT)<32000 THEN '(Small)'
              WHEN ColumnType = 'JN' THEN '(Large)'
          END as Column_Category_Ext
        ,COUNT(*) as ColumnCount
    FROM dbc.ColumnsV
    WHERE ColumnType IS NOT NULL
    GROUP BY 1,2,3,4,5,6
 ) C
  FULL OUTER JOIN dim_datatype DT
   ON DT.DataType = C.ColumnType
  AND DT.StorageFormat = C.StorageFormat
)with data 
no primary index on commit preserve rows;

/*{{save:dat_dbobject_count_per_column_type.csv}}*/
Select'{siteid}' as Site_ID, CURRENT_DATE as LogDate, ColumnType, ColumnCategory
, SUM(ColumnCount) AS Total_Cnt
from column_types
where columncount > 0
group by 3,4;

/*{{save:dat_dbobject_count_per_column_category.csv}}*/
select '{siteid}' as Site_ID
,ColumnCategory as "Column Category"
,sum(ColumnCount) as "Count Defined"
,rank() over(order by "Count Defined" desc) as "Rank"
from column_types
where columncount > 0
group by 2;

/*{{save:dat_dbobject_count_per_column_format.csv}}*/
SELECT '{siteid}' as Site_ID
,CASE WHEN FormatInd ='Y' THEN 'FORMATTED' ELSE 'NO DEFAULT FORMAT' END AS Column_Format
,sum(ColumnCount) as Total_Cnt
,cast(cast(Total_Cnt as format 'ZZZ,ZZZ,ZZ9') as varchar(20)) as TotalCntFmt
from column_types
where columncount > 0
group by 2
order by Column_Format;

/*{{save:dat_dbobject_column_count.csv}}*/
select trim(cast(count(distinct databasename) as INT format'ZZZ,ZZZ,ZZZ,ZZZ')) as Database_Cnt
,trim(cast(count(distinct databasename||tablename) as INT format'ZZZ,ZZZ,ZZZ,ZZZ')) as Table_Cnt
,trim(cast(count(*) as INT format'ZZZ,ZZZ,ZZZ,ZZZ')) as Column_Cnt
from dbc.ColumnsV;
