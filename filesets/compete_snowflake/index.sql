select 
  CASE
        WHEN Inds.IndexType = 'P'
            AND Inds.UniqueFlag = 'Y'
            THEN 'Unique Primary Index (UPI)'
        WHEN Inds.IndexType = 'P'
            AND Inds.UniqueFlag = 'N'
            THEN 'Non-Unique Primary Index (NUPI)'
        WHEN Inds.IndexType = 'Q'
            THEN 'Partitioned Primary Index'
        WHEN Inds.IndexType = 'A'
            THEN 'Primary AMP Index'
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
        WHEN Inds.IndexType = 'N'
        	THEN 'HASH Index'
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
        WHEN Inds.IndexType = 'G'
            THEN 'Geospatial nonunique secondary index'
        WHEN Inds.IndexType IS NULL
            THEN ' '
        ELSE
            Inds.IndexType
    END AS IndexTypeDesc, count(*) as Total
    from
    DBC.IndicesV Inds
    Where DatabaseName NOT IN ('All', 'Crashdumps', 'DBC', 'dbcmngr', 
        'Default', 'External_AP', 'EXTUSER', 'LockLogShredder', 'PUBLIC',
        'Sys_Calendar', 'SysAdmin', 'SYSBAR', 'SYSJDBC', 'SYSLIB', 
        'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'TD_SERVER_DB', 'TDStats',
        'TD_SYSGPL', 'TD_SYSXML', 'TDMaps', 'TDPUSER', 'TDQCD',
        'tdwm', 'SQLJ', 'TD_SYSFNLIB', 'SYSSPATIAL')
    group by 1;