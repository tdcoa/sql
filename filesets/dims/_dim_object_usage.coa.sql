/* Object Type Usage */                      
/*{{temp:dim_dbobject.csv}}*/;
create volatile table object_usage_per_type as 
(
SELECT     '{siteid}' as Site_ID    ,COALESCE(FT.ObjectType, OT.ObjectType) AS ObjectType    ,COALESCE(OT.ObjectTypeDesc, 'Unknown') AS ObjectTypeDesc    ,ZEROIFNULL(Frequency_of_Use) AS FrequencyofUseFROM    "dim_dbobject.csv" FT    FULL OUTER JOIN    (       SELECT            ObjectType            ,SUM(CAST(FreqofUse AS BIGINT)) AS Frequency_of_Use        FROM            {dbqlogtbl} OT        WHERE            LogDate BETWEEN {startdate} AND {enddate}          GROUP BY 1    ) OT    ON FT.ObjectType = OT.ObjectType) with data no primary index on commit preserve rows;