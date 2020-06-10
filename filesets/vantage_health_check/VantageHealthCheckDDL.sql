/*
##########################################
Below are Reference Tables.
Create an excel workbook for every reference table with listed columns.
The excel workbook will be imported and joined in tableau.

a) Table Cross REFERENCE

CREATE MULTISET TABLE system_fe.ca_table_xref ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      DataDomain VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      SubjectArea VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      DatabaseName VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      TableName VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX ( DatabaseName ,TableName );


b) User Cross REFERENCE

CREATE MULTISET TABLE systemfe.ca_user_xref ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      Username VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Department VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      SubDepartment VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
      Region VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC)
PRIMARY INDEX ( Username );

c) Feature Usage REFERENCE

CREATE SET TABLE consumption_feature_usage_v1 ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      SiteID VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LogDate DATE FORMAT 'yyyy-mm-dd',
      MaskedUserName BYTE(4),
      FeatureName VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      FeatureUseCount INTEGER)
PRIMARY INDEX ( SiteID ,LogDate ,MaskedUserName ,FeatureName );

#######################################################################
*/
-----------------------------------------------------------------------

/*
#######################################################################
Query1) Department CPU Consumption
#######################################################################
*/

	SELECT
	D.LogDate
	,StatementType
	,CASE WHEN StatementType = 'Merge Into' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Begin Loading' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Mload' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Collect Statistics' THEN 'Data Maintenance'
		  WHEN StatementType = 'Delete' THEN 'Ingest & Prep'
		  WHEN StatementType = 'End Loading' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Begin Delete Mload' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Update' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Select' THEN 'Answers'
		  WHEN StatementType = 'Exec' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Release Mload' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Insert' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Begin Mload' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Execute Mload' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Commit Work' THEN 'Ingest & Prep'
		  ELSE 'System/Procedural' END AS StatementOutcome
	,D.username as Username
	,UPPER(D.Username) as Department
	,UPPER(D.Username) as BusinessGroup
	,SUM(D.AMPCPUTime + D.ParserCPUTime) (BIGINT) as SUMCPUTime
	,COUNT(*) as QueryCount
	FROM PDCRINFO.DBQLogTbl_Hst D
	/*INNER JOIN systemfe.ca_user_xref U
		ON D.UserName = U.UserName*/
	WHERE D.LogDate BETWEEN current_date -30 AND current_date -1
	GROUP BY 1,2,3,4,5,6;
-----------------------------------------------------------------------
	


/*
#######################################################################
Query2)
The following query calculates how much CPU is consumed by a business group when it accesses a certain data domain. 
The amount of CPU consumed is equally distributed across the number of subject areas accessed.
How much CPU is consumed by a department when it accesses a certain data domain. 
The amount of CPU consumed is equally distributed across the number of data domains accessed.
how much CPU is consumed by a business group when it accesses a certain data domain.  
The amount of CPU consumed is equally distributed across the number of data domains accessed.
The followng query is made for excel version lookup table so we have aggreagetd by Username, Databasename, Tablename

#######################################################################
*/
SELECT 
SubDepartment as BusinessGroup
,DataDomain
,SubjectArea
,ObjectName
,SUM(CPUTimeBusinessGroup) CPUTimeBusinessGroup
,SUM(CPUTimeDataDomain) CPUTimeDataDomain
,SUM(CPUTimeSubjectArea) CPUTimeSubjectArea
,SUM(CPUTimeDatabasebTable) CPUTimeDatabasebTable

FROM(

    SELECT
    D.QueryID
	,D.SUMCPUTime/A.SubjectAreaCNT AS CPUTimeSubjectArea
	,D.SUMCPUTime/A.DataDomainCNT AS CPUTimeDataDomain
	,D.SUMCPUTime/A.ObjectNameCNT AS CPUTimeDatabasebTable
	,D.SUMCPUTime as CPUTimeBusinessGroup
    ,D.username as SubDepartment
	,o.ObjectDatabaseName as DataDomain
	,o.ObjectTableName as SubjectArea
    ,o.ObjectName as ObjectName
	

    FROM

        (SELECT
        QueryID
        ,LogDate
        ,(AMPCPUTime + ParserCPUTime) as SUMCPUTime
		,username as username
        --,Department
        --,SubDepartment
        FROM PDCRINFO.DbqlogTbl_Hst DBQL --INNER JOIN systemfe.ca_user_xref U
        --    ON DBQL.username = U.username
        WHERE Logdate BETWEEN current_date - 30 AND current_date -1) as D

    INNER JOIN 

        (SELECT
        QueryId,
        LogDate,
        ObjectDatabaseName,
        ObjectTableName,
		ObjectDatabaseName||'.'||ObjectDatabaseName AS ObjectName
        --DataDomain,
        --SubjectArea
        FROM PDCRINFO.DBQLObjTbl_Hst --INNER JOIN systemfe.ca_table_xref
        --        ON ObjectDatabaseName = DatabaseName
        --        AND ObjectDatabaseName = Tablename
        WHERE Logdate BETWEEN current_date - 30 AND current_date -1 
        AND ObjectType = 'Tab') as O

            ON D.QueryID = O.QueryID
            AND D.Logdate = O.Logdate

    INNER JOIN 
    
        (SELECT 
        QueryID
        ,COUNT(DISTINCT ObjectDatabaseName) DataDomainCNT
        ,COUNT(DISTINCT ObjectTableName) SubjectAreaCNT
		,COUNT(DISTINCT ObjectName) ObjectNameCNT
        FROM ( SELECT
                QueryId,
                ObjectDatabaseName,
                ObjectTableName,
                ObjectDatabaseName||'.'||ObjectDatabaseName AS ObjectName
				--DataDomain,
                --SubjectArea
                FROM PDCRINFO.DBQLObjTbl_Hst --INNER JOIN systemfe.ca_table_xref
                --        ON ObjectDatabaseName = DatabaseName
                --        AND ObjectTableName = Tablename
                WHERE Logdate BETWEEN current_date - 30 AND current_date -1 
                AND ObjectType = 'Tab') as A GROUP BY 1)  as A

    ON A.QueryID = D.QueryID
    GROUP BY 1,2,3,4,5,6,7,8,9
                                    ) as F
GROUP BY 1,2,3,4;

-----------------------------------------------------------------------

/*
#######################################################################
The followng is based on reference tables created in database.

#######################################################################
*/	
	
/*


--DeptDataDomainCPUConsumption

SELECT SUM(CPUTime)
,Department as Department
,DataDomain

FROM(

    SELECT
    D.D.QueryID
    ,D.SUMCPUTime/A.DataDomainCNT AS CPUTime
    ,Department
    ,DataDomain

    FROM

        (SELECT
        QueryID
        ,LogDate
        ,(AMPCPUTime + ParserCPUTime) as SUMCPUTime
        ,Department
        ,SubDepartment
        FROM PDCRINFO.DbqlogTbl_Hst DBQL INNER JOIN systemfe.ca_user_xref U
            ON DBQL.username = U.username
        WHERE Logdate BETWEEN current_date -30 and current_date -1) as D

    INNER JOIN 

        (SELECT
        QueryId,
        LogDate,
        DatabaseName,
        TableName,
        DataDomain,
        SubjectArea
        FROM PDCRINFO.DBQLObjTbl_Hst INNER JOIN systemfe.ca_table_xref
                ON ObjectDatabaseName = DatabaseName
                AND ObjectTableName = Tablename
        WHERE Logdate BETWEEN current_date -30 and current_date -1
        AND ObjectType = 'Tab') as O

            ON D.QueryID = O.QueryID
            AND D.Logdate = O.Logdate

    INNER JOIN 
    
        (SELECT 
        QueryID
        ,COUNT(DISTINCT DataDomain) DataDomainCNT
        ,COUNT(DISTINCT SubjectArea) SubjectAreaCNT
        FROM ( SELECT
                QueryId,
                DatabaseName,
                TableName,
                DataDomain,
                SubjectArea
                FROM PDCRINFO.DBQLObjTbl_Hst INNER JOIN systemfe.ca_table_xref
                        ON ObjectDatabaseName = DatabaseName
                        AND ObjectTableName = Tablename
                WHERE Logdate BETWEEN current_date -30 and current_date -1
                AND ObjectType = 'Tab') as A GROUP BY 1)  as A

    ON A.QueryID = D.QueryID
    GROUP BY 1,2,3,4
                                    ) as F
GROUP BY 2,3;

--	BusGrpDataDomainCPUConsumption
SELECT SUM(CPUTime)
,SubDepartment as BusinessGroup
,DataDomain

FROM(

    SELECT
    D.QueryID
    ,D.SUMCPUTime/A.DataDomainCNT AS CPUTime
    ,SubDepartment
    ,DataDomain

    FROM

        (SELECT
        QueryID
        ,LogDate
        ,(AMPCPUTime + ParserCPUTime) as SUMCPUTime
        ,Department
        ,SubDepartment
        FROM PDCRINFO.DbqlogTbl_Hst DBQL INNER JOIN systemfe.ca_user_xref U
            ON DBQL.username = U.username
        WHERE Logdate BETWEEN current_date -30 and current_date -1) as D

    INNER JOIN 

        (SELECT
        QueryId,
        LogDate,
        DatabaseName,
        TableName,
        DataDomain,
        SubjectArea
        FROM PDCRINFO.DBQLObjTbl_hst INNER JOIN systemfe.ca_table_xref
                ON ObjectDatabaseName = DatabaseName
                AND ObjectTableName = Tablename
        WHERE Logdate BETWEEN current_date -30 and current_date -1
        AND ObjectType = 'Tab') as O

            ON D.QueryID = O.QueryID
            AND D.Logdate = O.Logdate

    INNER JOIN 
    
        (SELECT 
        QueryID
        ,COUNT(DISTINCT DataDomain) DataDomainCNT
        ,COUNT(DISTINCT SubjectArea) SubjectAreaCNT
        FROM ( SELECT
                QueryId,
                DatabaseName,
                TableName,
                DataDomain,
                SubjectArea
                FROM PDCRINFO.DBQLObjTbl_Hst INNER JOIN systemfe.ca_table_xref
                        ON ObjectDatabaseName = DatabaseName
                        AND ObjectTableName = Tablename
                WHERE Logdate BETWEEN current_date -30 and current_date -1 
                AND ObjectType = 'Tab') as A GROUP BY 1)  as A

    ON A.QueryID = D.QueryID
    GROUP BY 1,2,3,4
                                    ) as F
GROUP BY 2,3;);


--BusGrpSubAreaCPUConsumption 

SELECT SUM(CPUTime)
,SubDepartment as BusinessGroup
,SubjectArea

FROM(

    SELECT
    D.QueryID
    ,D.SUMCPUTime/A.SubjectAreaCNT AS CPUTime
    ,SubDepartment
    ,SubjectArea

    FROM

        (SELECT
        QueryID
        ,LogDate
        ,(AMPCPUTime + ParserCPUTime) as SUMCPUTime
        ,Department
        ,SubDepartment
        FROM PDCRINFO.DbqlogTbl_hst DBQL INNER JOIN systemfe.ca_user_xref U
            ON DBQL.username = U.username
        WHERE Logdate BETWEEN current_date -30 and current_date -1) as D

    INNER JOIN 

        (SELECT
        QueryId,
        LogDate,
        DatabaseName,
        TableName,
        DataDomain,
        SubjectArea
        FROM PDCRINFO.DBQLObjTbl_hst INNER JOIN systemfe.ca_table_xref
                ON ObjectDatabaseName = DatabaseName
                AND ObjectTableName = Tablename
        WHERE Logdate BETWEEN current_date -30 and current_date -1
        AND ObjectType = 'Tab') as O

            ON D.QueryID = O.QueryID
            AND D.Logdate = O.Logdate

    INNER JOIN 
    
        (SELECT 
        QueryID
        ,COUNT(DISTINCT DataDomain) DataDomainCNT
        ,COUNT(DISTINCT SubjectArea) SubjectAreaCNT
        FROM ( SELECT
                QueryId,
                DatabaseName,
                TableName,
                DataDomain,
                SubjectArea
                FROM PDCRINFO.DBQLObjTbl_Hst INNER JOIN systemfe.ca_table_xref
                        ON ObjectDatabaseName = DatabaseName
                        AND ObjectTableName = Tablename
                WHERE Logdate BETWEEN current_date -30 and current_date -1
                AND ObjectType = 'Tab') as A GROUP BY 1)  as A

    ON A.QueryID = D.QueryID
    GROUP BY 1,2,3,4
                                    ) as F
GROUP BY 2,3;);


*/

-----------------------------------------------------------------------

/*
#######################################################################
Query3) 
The below query is for Feature Consumption Cross reference. 
The username wil be joined to ca_user_xref to aggregate by SubDepartment and Department
#######################################################################
*/


SELECT 
 A.LogDate as LogDate,
 A.USERNAME as MaskedUserName,
--  HASHROW(A.USERNAME) as MaskedUserName,
 CAST(B.FEATURENAME AS VARCHAR(100)) AS FeatureName, 
 SUM(GETBIT(A.FEATUREUSAGE,(2047 - B.FEATUREBITPOS))) AS FeatureUseCount, 
  ,COUNT(*) AS RequestCount
FROM PDCRINFO.DBQLOGTBL_HST A, 
     DBC.QRYLOGFEATURELISTV B 
WHERE LogDate BETWEEN current_date - 30  AND current_date 
GROUP BY 
    LogDate,
    USERNAME, 
    FeatureName having FeatureUseCount > 0
    ORDER BY 1,2,3;
	
-----------------------------------------------------------------------

/*
#######################################################################
Query4) 
consumption_ux_p1_v3 and consumption_ux_p1_v4 datasheets
#######################################################################
*/

SELECT					
    LogDate					
    ,LogHour					
  --,DayOfWeek as "Day Of Week"					
    ,WorkLoadType as "Workload Type"					
	,QueryType as "Query Type"				
	,StatementOutcome as "Statement Outcome"				
    ,QueryOrigin as "Query Origin"					
    ,DelaySeconds_Class as "Delay Seconds Group"					
    ,Parse_Time_Class as "Parse Time Group"					
    ,Execution_Time_Class as "Execution Time Group"					
    ,Transfer_Time_Class as "Transfer Time Group"					
    ,AMPCPUTime_Class  as "CPU Group"					
    ,ParserCPUTime_Class as "Parse CPU Group"					
    ,TotalIOCount_Class as "I/O Group"					
  --,IO_Optimization as "I/O Optimization"					
    ,CacheMissIOPSScore as "Cache Miss Count Score"   					
    ,CacheMissKBScore   as "Cache Miss Volume Score"					
    ,Complexity_Effect as "Complexity Effect"					
	,COMPLEXITY_Effect_Step as "Complexity Effect Step" 				
	,CASE  				
		WHEN COMPLEXITY_Effect_Step BETWEEN 0  and 1 THEN '1. Simple'			
		WHEN COMPLEXITY_Effect_Step >1 and COMPLEXITY_Effect_Step <=2 THEN '2. Medium'			
		WHEN COMPLEXITY_Effect_Step >2 and COMPLEXITY_Effect_Step <=3 THEN '3. Complex'			
		WHEN COMPLEXITY_Effect_Step >3 THEN '4. Very Complex'			
	END as COMPLEXITY				
  --,COMPLEXITY_CPU as "CPU Complexity"					
  --,COMPLEXITY_IO as "I/O Complexity"					
    ,COUNT(*) AS "Request Count" 					
    ,SUM(AMPCPUTime) AS "Total AMPCPUTime"					
    ,SUM(TotalIOCount) AS "Total IOCount"					
    ,SUM(ReqIOKB) AS "Total ReqIOKB"					
    ,SUM(ReqPhysIO) AS "Total ReqPhysIO"					
    ,SUM(ReqPhysIOKB) AS "Total ReqPhysIOKB"
	,SUM(SumLogIO_GB) as "Total ReqIO GB"  
	,SUM(SumPhysIO_GB) AS "Total ReqPhysIOGB"
  --,AVG(AMPCPUTime) AS "AVG AMPCPUTime"					
  --,AVG(TotalIOCount) AS "AVG IOCount"					
  --,AVG(ReqIOKB) AS "AVG ReqIOKB"					
  --,AVG(ReqPhysIO) AS "AVG ReqPhysIO"					
  --,AVG(ReqPhysIOKB) AS "AVG ReqPhysIOKB"					
  --,MIN(AMPCPUTime) AS "MIN AMPCPUTime"					
  --,MIN(TotalIOCount) AS "MIN IOCount"					
  --,MIN(ReqIOKB) AS "MIN ReqIOKB"					
  --,MIN(ReqPhysIO) AS "MIN ReqPhysIO"					
  --,MIN(ReqPhysIOKB) AS "MIN ReqPhysIOKB"					
  --,MAX(AMPCPUTime) AS "MAX AMPCPUTime"					
  --,MAX(TotalIOCount) AS "MAX IOCount"					
  --,MAX(ReqIOKB) AS "MAX ReqIOKB"					
  --,MAX(ReqPhysIO) AS "MAX ReqPhysIO"					
  --,MAX(ReqPhysIOKB) AS "MAX ReqPhysIOKB"					
    ,SUM(TotalServerByteCount) AS "Total Server Byte Count"					
FROM					
    (					
        SELECT					
            QryLog.LogDate					
            ,EXTRACT(HOUR FROM StartTime) AS LogHour					
            ,					
            CASE QryCal.day_of_week					
                WHEN 1					
                    THEN 'Sunday'					
                WHEN 2					
                    THEN 'Monday'					
                WHEN 3					
                    THEN 'Tuesday'					
                WHEN 4					
                    THEN 'Wednesday'					
                WHEN 5					
                    THEN 'Thursday'					
                WHEN 6					
                    THEN 'Friday'					
                WHEN 7					
                    THEN 'Saturday'					
            END AS DayOfWeek					
            ,QryLog.UserName					
            ,QryLog.AcctString					
            ,QryLog.AppID					
			,QryLog.NumSteps		
			,QryLog.NumStepswPar 		
            ,QryLog.MaxStepsInPar					
            ,HASHAMP() + 1 AS Total_AMPs					
            ,QryLog.QueryID					
            ,QryLog.StatementType					
			/* Request Groupings */            		
            ,CASE					
                WHEN QryLog.AppID LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%')        THEN 'LOAD'					
                WHEN QryLog.StatementType IN ('Insert', 'Update', 'Delete', 'Create Table', 'Merge Into') 					
                 AND QryLog.AppID NOT LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%')    THEN 'ETL/ELT'					
                WHEN QryLog.StatementType = 'Select' AND (AppID IN ('TPTEXP', 'FASTEXP') or appid like  'JDBCE%')         THEN 'EXPORT'					
                WHEN QryLog.StatementType = 'Select'					
                    AND QryLog.AppID NOT LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%') THEN 'QUERY'					
                WHEN QryLog.StatementType in ('Dump Database','Unrecognized type','Release Lock','Collect Statistics')    THEN 'ADMIN'                  					
                                                                                                                          ELSE 'OTHER'					
            END AS WorkLoadType					
			,CASE WHEN StatementType = 'Merge Into' THEN 'Ingest & Prep'		
				  WHEN StatementType = 'Begin Loading' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'Mload' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'Collect Statistics' THEN 'Data Maintenance'	
				  WHEN StatementType = 'Delete' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'End Loading' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'Begin Delete Mload' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'Update' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'Select' THEN 'Answers'	
				  WHEN StatementType = 'Exec' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'Release Mload' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'Insert' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'Begin Mload' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'Execute Mload' THEN 'Ingest & Prep'	
				  WHEN StatementType = 'Commit Work' THEN 'Ingest & Prep'	
				  ELSE 'System/Procedural' 	
			END AS StatementOutcome		
					
					
            /*,					
            CASE					
                WHEN (QryLog.StatementType IN ('Insert', 'Update', 'Delete', 'Create Table', 'Merge Into')					
					OR QryLog.AppID LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%'))
                    THEN 'ETL/ELT'					
                WHEN QryLog.StatementType = 'Select'					
                    AND (AppID IN ('TPTEXP', 'FASTEXP') or appid like  'JDBCE%')   					
                    THEN 'EXPORT'					
                WHEN QryLog.StatementType = 'Select'					
                    AND QryLog.AppID NOT LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%', 'JDBCE%')					
                    THEN 'QUERY'					
				--WHEN QryLog.StatementType in ('Dump Database','Unrecognized type','Release Lock','Collect Statistics')	
				    --THEN 'ADMIN'	
                ELSE					
                    'OTHER'					
            END AS WorkLoadType*/					
            ,					
            CASE					
                WHEN StatementType = 'Select'					
                    AND AppID NOT IN ('TPTEXP', 'FASTEXP')					
                    AND Execution_Time_Secs < 1					
                    AND NumOfActiveAMPs < Total_AMPs					
                    THEN 'Tactical'					
                ELSE					
                    'Non-Tactical'					
            END AS QueryType					
            ,					
            CASE					
                WHEN TotalServerByteCount > 0					
                    THEN 'QueryGrid'					
                ELSE					
                    'Local'					
            END AS QueryOrigin					
            ,QryLog.NumOfActiveAMPs					
            ,QryLog.AMPCPUTime					
					
            ,QryLog.TotalIOCount					
			,QryLog.ReqIOKB 		
            ,QryLog.ReqPhysIO 					
            ,QryLog.ReqPhysIOKB 					
			,QryLog.ParserCPUTime		
			,QryLog.CacheFlag		
			,QryLog.DelayTime		
			,QryLog.TotalServerByteCount		
			,(select MAX(QryLog.AMPCPUTime) FROM PDCRINFO.DBQLogTbl_Hst QryLog WHERE LogDate BETWEEN current_date - 30  AND current_date - 1 AND StartTime IS NOT NULL) as MAXCPU   --Observed CPU Cieling		
			,(select MAX(QryLog.TotalIOCount) FROM PDCRINFO.DBQLogTbl_Hst QryLog WHERE LogDate BETWEEN current_date - 30  AND current_date - 1 AND StartTime IS NOT NULL) as MAXIO  --Observed I/O Ceiling		
			,(select MAX(QryLog.NumSteps) FROM PDCRINFO.DBQLogTbl_Hst QryLog WHERE LogDate BETWEEN current_date - 30  AND current_date - 1 AND StartTime IS NOT NULL) as MAXSTEPS --Observed NumSteps Ceiling		
					
			,		
			CASE  		
			    WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*0)  and ((MAXCPU/10)*1) THEN 0		
				WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*1)  and ((MAXCPU/10)*2) THEN 1	
				WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*2)  and ((MAXCPU/10)*3) THEN 2	
				WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*3)  and ((MAXCPU/10)*4) THEN 3	
				WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*4)  and ((MAXCPU/10)*5) THEN 4	
				WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*5)  and ((MAXCPU/10)*6) THEN 5	
				WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*6)  and ((MAXCPU/10)*7) THEN 6	
				WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*7)  and ((MAXCPU/10)*8) THEN 7	
				WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*8)  and ((MAXCPU/10)*9) THEN 8	
				WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*9)  and ((MAXCPU/10)*10) THEN 9	
				WHEN QryLog.AMPCPUTime > ((MAXCPU/10)*10) THEN 10	
			END as COMPLEXITY_CPU		
			,		
			CASE  		
			    WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*0)  and ((MAXIO/10)*1) THEN 0		
				WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*1)  and ((MAXIO/10)*2) THEN 1	
				WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*2)  and ((MAXIO/10)*3) THEN 2	
				WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*3)  and ((MAXIO/10)*4) THEN 3	
				WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*4)  and ((MAXIO/10)*5) THEN 4	
				WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*5)  and ((MAXIO/10)*6) THEN 5	
				WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*6)  and ((MAXIO/10)*7) THEN 6	
				WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*7)  and ((MAXIO/10)*8) THEN 7	
				WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*8)  and ((MAXIO/10)*9) THEN 8	
				WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*9)  and ((MAXIO/10)*10) THEN 9	
				WHEN QryLog.TotalIOCount > ((MAXIO/10)*10) THEN 10	
			END as COMPLEXITY_IO		
			,		
			CASE  		
			    WHEN QryLog.NumSteps BETWEEN ((MAXSTEPS/10)*0)  and ((MAXSTEPS/10)*1) THEN 0		
				WHEN QryLog.NumSteps BETWEEN ((MAXSTEPS/10)*1)  and ((MAXSTEPS/10)*2) THEN 1	
				WHEN QryLog.NumSteps BETWEEN ((MAXSTEPS/10)*2)  and ((MAXSTEPS/10)*3) THEN 2	
				WHEN QryLog.NumSteps BETWEEN ((MAXSTEPS/10)*3)  and ((MAXSTEPS/10)*4) THEN 3	
				WHEN QryLog.NumSteps BETWEEN ((MAXSTEPS/10)*4)  and ((MAXSTEPS/10)*5) THEN 4	
				WHEN QryLog.NumSteps BETWEEN ((MAXSTEPS/10)*5)  and ((MAXSTEPS/10)*6) THEN 5	
				WHEN QryLog.NumSteps BETWEEN ((MAXSTEPS/10)*6)  and ((MAXSTEPS/10)*7) THEN 6	
				WHEN QryLog.NumSteps BETWEEN ((MAXSTEPS/10)*7)  and ((MAXSTEPS/10)*8) THEN 7	
				WHEN QryLog.NumSteps BETWEEN ((MAXSTEPS/10)*8)  and ((MAXSTEPS/10)*9) THEN 8	
				WHEN QryLog.NumSteps BETWEEN ((MAXSTEPS/10)*9)  and ((MAXSTEPS/10)*10) THEN 9	
				WHEN QryLog.NumSteps > ((MAXSTEPS/10)*10) THEN 10	
			END as COMPLEXITY_NUMSTEPS		
					
			,(((COMPLEXITY_CPU + COMPLEXITY_IO + COMPLEXITY_NUMSTEPS + 0.7)/3)(DECIMAL(6,0))) as COMPLEXITY_Effect_Step		
			,(((COMPLEXITY_CPU + COMPLEXITY_IO + 0.5)/2) (DECIMAL(6,0))) as COMPLEXITY_Effect	
			,		
            CASE					
                WHEN DelayTime is NULL					
                    THEN '0000 - 0000'					
				WHEN DelayTime < 1.0 or DelayTime is NULL	
                    THEN '0000 - 0001'					
                WHEN DelayTime BETWEEN 1.0 AND 5.0					
                    THEN '0001 - 0005'					
                WHEN DelayTime BETWEEN 5.0 AND 10.0					
                    THEN '0005 - 0010'					
                WHEN DelayTime BETWEEN 10.0 AND 30.0					
                    THEN '0010 - 0030'					
                WHEN DelayTime BETWEEN 30.0 AND 60.0					
                    THEN '0030 - 0060'					
                WHEN DelayTime BETWEEN 60.0 AND 300.0					
                    THEN '0060 - 0300'					
                WHEN DelayTime BETWEEN 300.0 AND 600.0					
                    THEN '0300 - 0600'					
                WHEN DelayTime BETWEEN 600.0 AND 1800.0					
                    THEN '0600 - 1800'					
                WHEN DelayTime BETWEEN 1800.0 AND 3600.0					
                    THEN '1800 - 3600'					
                WHEN DelayTime > 3600.0					
                    THEN '3600+'					
            END AS DelaySeconds_Class					
            ,((FirstRespTime - StartTime) HOUR(3) TO SECOND(6)) AS Execution_Time					
            ,((FirstStepTime - StartTime) HOUR(3) TO SECOND(6)) AS Parse_Time					
            ,((COALESCE(LastRespTime,FirstRespTime) - FirstRespTime) HOUR(3) TO SECOND(6)) AS Transfer_Time					
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Execution_Time) * 3600 + EXTRACT(MINUTE FROM Execution_Time) * 60 + EXTRACT(SECOND FROM Execution_Time) AS FLOAT)) AS Execution_Time_Secs					
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Transfer_Time) * 3600 + EXTRACT(MINUTE FROM Transfer_Time) * 60 + EXTRACT(SECOND FROM Transfer_Time) AS FLOAT)) AS Transfer_Time_Secs					
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Parse_Time) * 3600 + EXTRACT(MINUTE FROM Parse_Time) * 60 + EXTRACT(SECOND FROM Parse_Time) AS FLOAT)) AS Parse_Time_Secs					
			,		
            CASE					
                WHEN Parse_Time_Secs IS NULL					
                    THEN '0000 - 0000'					
				WHEN Parse_Time_Secs < 1.0	
                    THEN '0000 - 0001'					
                WHEN Parse_Time_Secs BETWEEN 1.0 AND 5.0					
                    THEN '0001 - 0005'					
                WHEN Parse_Time_Secs BETWEEN 5.0 AND 10.0					
                    THEN '0005 - 0010'					
                WHEN Parse_Time_Secs BETWEEN 10.0 AND 30.0					
                    THEN '0010 - 0030'					
                WHEN Parse_Time_Secs BETWEEN 30.0 AND 60.0					
                    THEN '0030 - 0060'					
                WHEN Parse_Time_Secs BETWEEN 60.0 AND 300.0					
                    THEN '0060 - 0300'					
                WHEN Parse_Time_Secs BETWEEN 300.0 AND 600.0					
                    THEN '0300 - 0600'					
                WHEN Parse_Time_Secs BETWEEN 600.0 AND 1800.0					
                    THEN '0600 - 1800'					
                WHEN Parse_Time_Secs BETWEEN 1800.0 AND 3600.0					
                    THEN '1800 - 3600'					
                WHEN Parse_Time_Secs > 3600.0					
                    THEN '3600+'					
            END AS Parse_Time_Class					
			,		
            CASE					
                WHEN AMPCPUTime IS NULL					
                    THEN '00000 - 000000'					
				WHEN AMPCPUTime < 1.0	
                    THEN '00000 - 000001'					
                WHEN AMPCPUTime BETWEEN 1.0 AND 10.0					
                    THEN '00001 - 000010'					
                WHEN AMPCPUTime BETWEEN 10.0 AND 100.0					
                    THEN '00010 - 000100'					
                WHEN AMPCPUTime BETWEEN 100.0 AND 1000.0					
                    THEN '00100 - 001000'					
                WHEN AMPCPUTime BETWEEN 1000.0 AND 10000.0					
                    THEN '01000 - 010000'					
                WHEN AMPCPUTime BETWEEN 10000.0 AND 100000.0					
                    THEN '10000 - 100000'					
                WHEN AMPCPUTime > 100000.0					
                    THEN '100000+'					
            END AS AMPCPUTime_Class					
			,		
            CASE					
                WHEN ParserCPUTime IS NULL					
                    THEN '00000 - 00000'					
				WHEN ParserCPUTime < 1.0	
                    THEN '00000 - 00001'					
                WHEN ParserCPUTime BETWEEN 1.0 AND 5.0					
                    THEN '00001 - 00005'					
                WHEN ParserCPUTime BETWEEN 5.0 AND 10.0					
                    THEN '00005 - 00010'					
                WHEN ParserCPUTime BETWEEN 10.0 AND 50.0					
                    THEN '00010 - 00050'					
                WHEN ParserCPUTime BETWEEN 50.0 AND 100.0					
                    THEN '00050 - 00100'					
                WHEN ParserCPUTime BETWEEN 100.0 AND 500.0					
                    THEN '00100 - 00500'					
                WHEN ParserCPUTime BETWEEN 500.0 AND 1000.0					
                    THEN '00500 - 01000'					
                WHEN ParserCPUTime BETWEEN 1000.0 AND 5000.0					
                    THEN '01000 - 05000'					
                WHEN ParserCPUTime BETWEEN 5000.0 AND 10000.0					
                    THEN '05000 - 10000'					
                WHEN ParserCPUTime > 10000.0					
                    THEN '10000+'					
            END AS ParserCPUTime_Class					
			,		
            CASE					
                WHEN TotalIOCount IS NULL					
                    THEN '1e0-1e0'					
				WHEN TotalIOCount < 1e4	
                    THEN '1e0-1e4'					
                WHEN TotalIOCount BETWEEN 1e4 AND 1e6					
                    THEN '1e4-1e6'					
                WHEN TotalIOCount BETWEEN 1e6 AND 1e8					
                    THEN '1e6-1e8'					
                WHEN TotalIOCount BETWEEN 1e8 AND 1e10					
                    THEN '1e8-1e10'					
                WHEN TotalIOCount > 1e10					
                    THEN '1e10+'					
            END AS TotalIOCount_Class					
            ,					
            CASE					
                WHEN Execution_Time_Secs IS NULL					
                    THEN '00000 - 000000'					
				WHEN Execution_Time_Secs < 1.0	
                    THEN '00000 - 000001'					
                WHEN Execution_Time_Secs BETWEEN 1.0 AND 1e1					
                    THEN '00001 - 000010'					
                WHEN Execution_Time_Secs BETWEEN 1e1 AND 1e2					
                    THEN '00010 - 000100'					
                WHEN Execution_Time_Secs BETWEEN 1e2 AND 1e3					
                    THEN '00100 - 001000'					
                WHEN Execution_Time_Secs BETWEEN 1e3 AND 1e4					
                    THEN '01000 - 010000'					
                WHEN Execution_Time_Secs > 1e4					
                    THEN '10000+'					
            END AS Execution_Time_Class					
            ,					
            CASE					
                WHEN TotalIOCount = 0					
                    THEN 'No I/O'					
				WHEN TotalIOCount > 0 AND ReqPhysIO = 0	
                    THEN 'In Memory'					
                WHEN TotalIOCount > 0 AND ReqPhysIO > 0					
                    THEN 'Physical I/O'					
            END AS IO_Optimization					
					
 					
 /*   IOPS  Metrics  */					
, (totaliocount)/1000 SumKio					
, (ReqPhysIO)/1000  SumPhysKioCnt					
, zeroifnull( SumPhysKioCnt/nullifzero(SumKio) )  CacheMissPctIOPS					
                    					
/*  IO Bytes Metrics  */					
, (ReqIOKB)/1e6 SumLogIO_GB					
, (ReqPhysIOKB)/1e6 SumPhysIO_GB					
, zeroifnull(SumPhysIO_GB/nullifzero(SumLogIO_GB))  CacheMissPctGB					
 					
 /* METRIC:   Cache Miss Rate IOPS.  normal cache miss rate <20%,   set score = 0  for  miss rate < 20%,  increments of 10%, range 0 -80 */  					
			,		
			case		
				when  SumPhysKioCnt = 0 then 0   	
                when   zeroifnull(SumPhysKioCnt/ nullifzero(SumKio)) <= 0.20 then 0                         /* set score = 0 when less than industry average 20% */					
                when   SumPhysKioCnt > SumKio then 80                                                       /* sometimes get Physical > Logical, set ceiling at 80*/					
                else (cast( 100 * zeroifnull (SumPhysKioCnt/ nullifzero(SumKio)) /10 as  integer) * 10) - 20  /* only count above 20%, round to bin size 10*/					
            end as CacheMissIOPSScore                    					
                    					
 /* METRIC:   Cache Miss Rate KB.  normal cache miss rate <20%,   set score = 0  for  miss rate < 20%,  increments of 10%, range 0 -80 */  					
   			,  		
   			case 		
				when  SumPhysIO_GB = 0 then 0   	
                when   zeroifnull(SumPhysIO_GB/ nullifzero(SumLogIO_GB)) <= 0.20 then 0                   /* set score = 0 when less than industry average 20% */					
                when   SumPhysIO_GB > SumLogIO_GB then 80                                  /* sometimes get Physical > Logical, set ceiling at 80*/					
                else  (cast( 100 * zeroifnull (SumPhysIO_GB/ nullifzero(SumLogIO_GB)) /10 as  integer) * 10) - 20   /* only count above 20%, round to bin size 10*/					
            end as CacheMissKBScore   					
            ,					
            CASE					
                WHEN Transfer_Time_Secs IS NULL 					
                    THEN '0000 - 0000'					
				WHEN Transfer_Time_Secs < 1.0	
                    THEN '0000 - 0001'					
                WHEN Transfer_Time_Secs BETWEEN 1.0 AND 5.0					
                    THEN '0001 - 0005'					
                WHEN Transfer_Time_Secs BETWEEN 5.0 AND 10.0					
                    THEN '0005 - 0010'					
                WHEN Transfer_Time_Secs BETWEEN 10.0 AND 30.0					
                    THEN '0010 - 0030'					
                WHEN Transfer_Time_Secs BETWEEN 30.0 AND 60.0					
                    THEN '0030 - 0060'					
                WHEN Transfer_Time_Secs BETWEEN 60.0 AND 300.0					
                    THEN '0060 - 0300'					
                WHEN Transfer_Time_Secs BETWEEN 300.0 AND 600.0					
                    THEN '0300 - 0600'					
                WHEN Transfer_Time_Secs BETWEEN 600.0 AND 1800.0					
                    THEN '0600 - 1800'					
                WHEN Transfer_Time_Secs BETWEEN 1800.0 AND 3600.0					
                    THEN '1800 - 3600'					
                WHEN Transfer_Time_Secs > 3600.0					
                    THEN '3600+'					
            END AS Transfer_Time_Class					
        FROM					
            PDCRINFO.DBQLogTbl_Hst QryLog					
            INNER JOIN					
            Sys_Calendar.CALENDAR QryCal					
                ON QryCal.calendar_date = QryLog.LogDate					
        WHERE					
            LogDate BETWEEN current_date - 30  AND current_date - 1					
            AND StartTime IS NOT NULL					
    ) AS QryDetails					
GROUP BY					
     1  --X					
    ,2					
    ,3					
    ,4					
    ,5					
    ,6					
    ,7					
    ,8					
    ,9					
	,10				
	,11				
	,12				
	,13				
	,13				
	,14				
	,15--				
	,16				
	,17				
	,18				
					
	--,16				
	--,17				
	--,18				
	--,19				
order by LogDate, LogHour, "Total AMPCPUTime" desc;					


Query5
--Concurrency Dashboard

Select				
 				
 /* Date/Time Columns */  				
XLogDate				
,XLogHour				
 /* Request Groupings */            				
,WorkLoadType				
,QueryType				
,CASE WHEN StatementType = 'Merge Into' THEN 'Ingest & Prep'				
				  WHEN StatementType = 'Begin Loading' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Mload' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Collect Statistics' THEN 'Data Maintenance'
				  WHEN StatementType = 'Delete' THEN 'Ingest & Prep'
				  WHEN StatementType = 'End Loading' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Begin Delete Mload' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Update' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Select' THEN 'Answers'
				  WHEN StatementType = 'Exec' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Release Mload' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Insert' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Begin Mload' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Execute Mload' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Commit Work' THEN 'Ingest & Prep'
				  ELSE 'System/Procedural' 
END AS StatementOutcome				
,QueryOrigin            				
,DelaySeconds_Class as "Delay Seconds Group"				
,Parse_Time_Class as "Parse Time Group"				
,Execution_Time_Class as "Execution Time Group"				
,Transfer_Time_Class as "Transfer Time Group"				
,AMPCPUTime_Class  as "CPU Group"				
,ParserCPUTime_Class as "Parse CPU Group"				
,TotalIOCount_Class as "I/O Group"				
--,IO_Optimization as "I/O Optimization"				
,CacheMissIOPSScore as "Cache Miss Count Score"   				
,CacheMissKBScore   as "Cache Miss Volume Score"				
,Complexity_Effect as "Complexity Effect"				
--,COMPLEXITY_CPU 				
--,COMPLEXITY_IO  				
 /* Concurrency */    				
,AVG(XC1) as Concurrency_1Minute_Avg_XC1				
,AVG(XC2) as Concurrency_1Minute_Avg_XC2				
--,SUM(XC2) over (partition by XlogDate,XLogHour) as Concurrency_Hourly_Avg				
,SUM(XC1) as Concurrency_Hourly_Avg_XC1				
,SUM(XC2) as Concurrency_Hourly_Avg_XC2				
--,AVG(XC2) over (partition by XlogDate,XLogHour,XLogMinute/10) as Concurrency_10Minute_Avg				
--,SUM(XC2) over (partition by XlogDate,XLogHour,XLogMinute) as Concurrency_01Minute_Total				
 /* Request CPU */  				
,SUM(XAmpCPUTime) as "X Amp CPUTime"				
,SUM(XImpactCPU) as "X Impact CPU"				
 /* Request Logical I/O  */  				
,SUM(XTotalIOCount) as "X Total IO Count"             				
,SUM(XTotalKio) as "X Total Kio"				
,SUM(XSumKIO) as "X Sum KIO"              				
,SUM(XImpactIO) as "X Impact IO"				
,SUM(XReqIOKB) as "X Req IOKB"           				
,SUM(XSumLogIO_GB) as "XSumLogIO GB"               				
,SUM(XLogicalIO_GB) as "XLogicalIO GB"				
 /* Request Physical I/O  */            				
,SUM(XReqPhysIO)   as "X Req Phys IO"	               			
,SUM(XPhysKioCnt) as "X Phys Kio Cnt"				
,SUM(XSumPhysKioCnt) as "X Sum Phys Kio Cnt"              				
,SUM(XReqPhysIOKB)  as "X Req Phys IOKB"  				
,SUM(XSumPhysIO_GB)  as "XSumPhysIO GB"         				
,SUM(XPhysIO_GB) as "XPhysIO GB"				
        				
--,AVG(AVGDelayTime) ---AVG over QueryID for ClockTick				
--,AVG(RespTime) ---AVG over QueryId for ClockTick   				
--,AVG(AVG_Execution_Time_Secs)  ---AVG over QueryId for ClockTick    				
--,AVG(AVG_Transfer_Time_Secs)  ---AVG over QueryId for ClockTick     				
--,AVG(AVG_Parse_Time_Secs)  ---AVG over QueryId for ClockTick    				
 /* Request Characteristics */  				
--,AVG(AVGServerByteCount)    as AvgServerByteCount				
--,AVG(AVGCacheMissPctIOPS) as AvgCacheMissPctIOPS				
--,AVG(AVGCacheMissPctGB) as AvgCacheMissPctGB    				
from				
(				
Select				
 /* Date/Time Columns */  				
 Cast(ClockTick as DATE) as XLogDate				
,EXTRACT(HOUR from ClockTick) as XLogHour				
,EXTRACT(MINUTE from ClockTick) as XLogMinute 				
 /* Request Groupings */            				
,WorkLoadType				
,QueryType				
,StatementType				
,QueryOrigin            				
,COMPLEXITY_CPU 				
,COMPLEXITY_IO  				
,COMPLEXITY_Effect          				
,DelaySeconds_Class				
,Parse_Time_Class				
,AMPCPUTime_Class				
,ParserCPUTime_Class        				
,TotalIOCount_Class                 				
,Execution_Time_Class				
,Transfer_Time_Class             				
,IO_Optimization				
,CacheMissIOPSScore                    				
,CacheMissKBScore  				
--,AVG(XC2) over (partition by XlogDate,XLogHour) as Concurrency_Hourly_Avg				
--,AVG(XC2) over (partition by XlogDate,XLogHour,XLogMinute/10) as Concurrency_10Minute_Avg				
--,SUM(XC2) over (partition by XlogDate,XLogHour,XLogMinute) as Concurrency_01Minute_Total				
,SUM(XConcurrency) as XC1				
,Count (distinct QueryID) as XC2				
 /* Request CPU */  				
,SUM(AmpCpuTime *  PctSpread) as XAmpCPUTime				
,SUM(ImpactCpu  *  PctSpread) as XImpactCPU				
 /* Request Logical I/O  */  				
,SUM(TotalIOCount *  PctSpread) as XTotalIOCount            				
,SUM(TotalKio *  PctSpread) as XTotalKio				
,SUM(SumKio *  PctSpread) as XSumKIO            				
,SUM(ImpactKio *  PctSpread) as XImpactIO				
,SUM(ReqIOKB *  PctSpread) as XReqIOKB      				
,SUM(SumLogIO_GB *  PctSpread) as XSumLogIO_GB          				
,SUM(LogicalIO_GB *  PctSpread) as XLogicalIO_GB				
 /* Request Physical I/O  */            				
,SUM(ReqPhysIO *  PctSpread) as XReqPhysIO          				
,SUM(PhysKioCnt *  PctSpread) as XPhysKioCnt				
,SUM(SumPhysKioCnt *  PctSpread) as XSumPhysKioCnt          				
,SUM(ReqPhysIOKB *  PctSpread) as XReqPhysIOKB  				
,SUM(SumPhysIO_GB *  PctSpread) as XSumPhysIO_GB            				
,SUM(PhysIO_GB *  PctSpread) as XPhysIO_GB				
        				
--,AVG(DelayTime) as AvgDelayTime---AVG over QueryID for ClockTick				
--,AVG(RespTime) ---AVG over QueryId for ClockTick   				
--,AVG(Execution_Time_Secs) as AVG_Execution_Time_Secs  ---AVG over QueryId for ClockTick     				
--,AVG(Transfer_Time_Secs) as Avg_Transfer_Time_secs ---AVG over QueryId for ClockTick    				
--,AVG(Parse_Time_Secs)  as Avg_Parse_Time_Secs ---AVG over QueryId for ClockTick     				
 /* Request Characteristics */  				
--,AVG(TotalServerByteCount)  as AvgServerByteCount				
--,AVG(CacheMissPctIOPS) as AvgCacheMissPctIOPS				
--,AVG(CacheMissPctGB) as AvgCacheMissPctGB   				
				
				
From (				
select   				
  QueryID				
, firststeptime				
, firstresptime as EndTime				
, ClockTick 				
, QryPeriod				
, ClockTick - firststeptime day to second as SMinSecs				
, ClockTick - EndTime day to second as EMinSecs				
, extract (second from SMinSecs) + (extract(minute from SMinSecs)*60) + (extract(hour from SMinSecs)*60*60) + (extract(day from SMinSecs)*86400) as Seconds1				
, extract (second from EMinSecs) + (extract(minute from EMinSecs)*60) + (extract(hour from EMinSecs)*60*60) + (extract(day from EMinSecs)*86400) as Seconds2				
, CASE 				
      WHEN Seconds1 < 60 and Seconds2 > 0 and Seconds2 < 60 then Seconds1 - Seconds2 --request entirely within one clocktick				
      WHEN Seconds1 < 60 THEN Seconds1 --portion of request before first clocktick 				
      WHEN Seconds2 >0 and Seconds2 < 60 THEN 60 - Seconds2 --portion of request in last clocktick 				
  ELSE 60 END as Seconds3  --request active for entire clocktick 				
, CAST(seconds3 as decimal(9,1)) as Seconds4				
, QrySecs  QryRunSecs				
, (ampcputime (DECIMAL(18,4)))  TotalQryCpu				
, CASE WHEN (seconds4/(QryRunSecs (DECIMAL(38,6)))) = 0 THEN TotalQryCpu				
       ELSE (seconds4/(QryRunSecs (DECIMAL(38,6)))) END as PctSpread --Percent of resources applied in period				
, (ampcputime)* PctSpread  ClockTickCPU				
,QrySecs				
,1 as XConcurrency				
 /* Date/Time Columns */  				
,LogDate				
,StartDate          				
,LogHour				
,DelayTime				
,starttime 				
,firstresptime				
,RespTime   				
,Execution_Time				
,Parse_Time         				
,Transfer_Time				
,Execution_Time_Secs				
,Transfer_Time_Secs				
,Parse_Time_Secs				
,DayOfWeek   				
 /* Request Characteristics */  				
,UserName				
,AcctString				
,AppID				
,NumSteps				
,NumStepswPar 				
,MaxStepsInPar				
,NumOfActiveAMPs            				
,Total_AMPs				
,StatementType				
,TotalServerByteCount   				
 /* Request CPU */  				
,AmpCpuTime  				
,ImpactCpu				
 /* Request Logical I/O  */  				
,TotalIOCount           				
,TotalKio				
,SumKio         				
,ImpactKio				
,ReqIOKB        				
,SumLogIO_GB            				
,LogicalIO_GB				
 /* Request Physical I/O  */            				
,ReqPhysIO          				
,PhysKioCnt				
,SumPhysKioCnt          				
,ReqPhysIOKB    				
,SumPhysIO_GB           				
,PhysIO_GB				
,CacheMissPctIOPS				
,CacheMissPctGB         				
,ParserCPUTime				
,CacheFlag				
 /* 30-Day Max CPU & I/O  */  				
,MAXCPU   --Observed CPU Cieling				
,MAXIO  --Observed I/O Ceiling				
 /* Request Groupings */            				
,WorkLoadType				
,QueryType				
,QueryOrigin            				
,COMPLEXITY_CPU 				
,COMPLEXITY_IO  				
,COMPLEXITY_Effect          				
,DelaySeconds_Class				
,Parse_Time_Class				
,AMPCPUTime_Class				
,ParserCPUTime_Class        				
,TotalIOCount_Class                 				
,Execution_Time_Class				
,Transfer_Time_Class             				
,IO_Optimization				
,CacheMissIOPSScore                    				
,CacheMissKBScore    				
 				
 From (  				
SELECT				
----- ----- ----- -----				
 /* EXPAND Execution Time (firststeptime,RespTime): 1 row for each 1-minute interval of execution time */  				
             BEGIN(Qper)  ClockTick				
            ,PERIOD(firststeptime,RespTime) QryPeriod				
            ,(CAST( EXTRACT (SECOND FROM FirstRespTime) + (EXTRACT (MINUTE FROM FirstRespTime) * 60 ) 				
            +(EXTRACT (HOUR FROM FirstRespTime) *60*60 ) + (86400 * (CAST ( FirstRespTime AS DATE) 				
            -CAST ( firststeptime AS DATE) ) )  - (EXTRACT (SECOND FROM firststeptime) 				
            +(EXTRACT (MINUTE FROM firststeptime) * 60 ) + (EXTRACT (HOUR FROM firststeptime) *60*60 ) ) AS decimal(18,4)) ) QrySecs            				
 /* Date/Time Columns */  				
            ,QryLog.LogDate				
            ,cast(QryLog.FirstStepTime as DATE) as StartDate            				
            ,EXTRACT(HOUR FROM QryLog.StartTime) AS LogHour				
            ,QryLog.DelayTime				
            ,QryLog.starttime 				
            ,QryLog.firststeptime				
            ,QryLog.firstresptime				
            ,firstresptime + INTERVAL '59.999999' SECOND as RespTime    				
            ,((QryLog.FirstRespTime - QryLog.StartTime) HOUR(3) TO SECOND(6)) AS Execution_Time				
            ,((QryLog.FirstStepTime - QryLog.StartTime) HOUR(3) TO SECOND(6)) AS Parse_Time         				
            ,((COALESCE(QryLog.LastRespTime,QryLog.FirstRespTime) - QryLog.FirstRespTime) HOUR(3) TO SECOND(6)) AS Transfer_Time				
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Execution_Time) * 3600 + EXTRACT(MINUTE FROM Execution_Time) * 60 + EXTRACT(SECOND FROM Execution_Time) AS FLOAT)) AS Execution_Time_Secs				
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Transfer_Time) * 3600 + EXTRACT(MINUTE FROM Transfer_Time) * 60 + EXTRACT(SECOND FROM Transfer_Time) AS FLOAT)) AS Transfer_Time_Secs				
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Parse_Time) * 3600 + EXTRACT(MINUTE FROM Parse_Time) * 60 + EXTRACT(SECOND FROM Parse_Time) AS FLOAT)) AS Parse_Time_Secs				
            ,CASE QryCal.day_of_week				
                WHEN 1 THEN 'Sunday'				
                WHEN 2 THEN 'Monday'				
                WHEN 3 THEN 'Tuesday'				
                WHEN 4 THEN 'Wednesday'				
                WHEN 5 THEN 'Thursday'				
                WHEN 6 THEN 'Friday'				
                WHEN 7 THEN 'Saturday'				
             END AS DayOfWeek				
             				
 /* Request Characteristics */  				
            ,QryLog.UserName				
            ,QryLog.AcctString				
            ,QryLog.AppID				
            ,QryLog.NumSteps				
            ,QryLog.NumStepswPar 				
            ,QryLog.MaxStepsInPar				
            ,QryLog.NumOfActiveAMPs         				
            ,HASHAMP() + 1 AS Total_AMPs				
            ,QryLog.QueryID				
            ,QryLog.StatementType				
            ,QryLog.TotalServerByteCount    				
            				
 /* Request CPU */  				
            ,QryLog.AmpCpuTime  				
            ,(QryLog.maxampcputime * QryLog.numofactiveamps) ImpactCpu				
            				
 /* Request Logical I/O  */  				
            ,QryLog.TotalIOCount            				
            ,QryLog.totaliocount/1000 TotalKio				
            ,QryLog.totaliocount/1000 SumKio            				
            ,(QryLog.maxampio *QryLog.numofactiveamps )/1000 ImpactKio				
            ,QryLog.ReqIOKB         				
            ,QryLog.ReqIOKB/1e6 SumLogIO_GB         				
            ,QryLog.ReqIOKB/1e6 LogicalIO_GB				
            				
 /* Request Physical I/O  */            				
            ,QryLog.ReqPhysIO           				
            ,QryLog.ReqPhysIO/1000 PhysKioCnt				
            ,QryLog.ReqPhysIO/1000  SumPhysKioCnt           				
            ,QryLog.ReqPhysIOKB 				
            ,QryLog.ReqPhysIOKB/1e6 SumPhysIO_GB            				
            ,QryLog.ReqPhysIOKB/1e6 PhysIO_GB				
            ,zeroifnull( SumPhysKioCnt/nullifzero(SumKio) )  CacheMissPctIOPS				
            ,zeroifnull(SumPhysIO_GB/nullifzero(SumLogIO_GB))  CacheMissPctGB           				
            ,QryLog.ParserCPUTime				
            ,QryLog.CacheFlag				
				
 /* 30-Day Max CPU & I/O  */  				
            ,(select MAX(QryLogX.AMPCPUTime) FROM PDCRINFO.DBQLogTbl_Hst QryLogX WHERE LogDate BETWEEN current_date - 30  AND current_date - 1 AND StartTime IS NOT NULL) as MAXCPU   --Observed CPU Cieling				
            ,(select MAX(QryLogX.TotalIOCount) FROM PDCRINFO.DBQLogTbl_Hst QryLogX WHERE LogDate BETWEEN current_date - 30  AND current_date - 1 AND StartTime IS NOT NULL) as MAXIO  --Observed I/O Ceiling				
            				
 /* Request Groupings */            				
            ,CASE				
                WHEN QryLog.AppID LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%')        THEN 'LOAD'				
                WHEN QryLog.StatementType IN ('Insert', 'Update', 'Delete', 'Create Table', 'Merge Into') 				
                 AND QryLog.AppID NOT LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%')    THEN 'ETL/ELT'				
                WHEN QryLog.StatementType = 'Select' AND (AppID IN ('TPTEXP', 'FASTEXP') or appid like  'JDBCE%')         THEN 'EXPORT'				
                WHEN QryLog.StatementType = 'Select'				
                    AND QryLog.AppID NOT LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%') THEN 'QUERY'				
                WHEN QryLog.StatementType in ('Dump Database','Unrecognized type','Release Lock','Collect Statistics')    THEN 'ADMIN'                  				
                                                                                                                          ELSE 'OTHER'				
            END AS WorkLoadType				
            ,CASE				
                WHEN StatementType = 'Select'				
                    AND AppID NOT IN ('TPTEXP', 'FASTEXP')				
                    AND Execution_Time_Secs < 1				
                    AND NumOfActiveAMPs < Total_AMPs				
                    THEN 'Tactical'				
                ELSE				
                    'Non-Tactical'				
            END AS QueryType				
            ,CASE				
                WHEN TotalServerByteCount > 0 THEN 'QueryGrid'				
                                              ELSE 'Local'				
             END AS QueryOrigin         				
            ,CASE  				
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*0)  and ((MAXCPU/10)*1) THEN 0				
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*1)  and ((MAXCPU/10)*2) THEN 1				
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*2)  and ((MAXCPU/10)*3) THEN 2              				
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*3)  and ((MAXCPU/10)*4) THEN 3              				
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*4)  and ((MAXCPU/10)*5) THEN 4              				
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*5)  and ((MAXCPU/10)*6) THEN 5              				
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*6)  and ((MAXCPU/10)*7) THEN 6              				
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*7)  and ((MAXCPU/10)*8) THEN 7				
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*8)  and ((MAXCPU/10)*9) THEN 8              				
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*9)  and ((MAXCPU/10)*10) THEN 9         				
                WHEN QryLog.AMPCPUTime > ((MAXCPU/10)*10) THEN 10				
             END as COMPLEXITY_CPU  				
            ,CASE  				
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*0)  and ((MAXIO/10)*1) THEN 0				
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*1)  and ((MAXIO/10)*2) THEN 1				
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*2)  and ((MAXIO/10)*3) THEN 2              				
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*3)  and ((MAXIO/10)*4) THEN 3              				
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*4)  and ((MAXIO/10)*5) THEN 4              				
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*5)  and ((MAXIO/10)*6) THEN 5              				
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*6)  and ((MAXIO/10)*7) THEN 6              				
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*7)  and ((MAXIO/10)*8) THEN 7				
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*8)  and ((MAXIO/10)*9) THEN 8              				
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*9)  and ((MAXIO/10)*10) THEN 9         				
                WHEN QryLog.TotalIOCount > ((MAXIO/10)*10) THEN 10				
            END as COMPLEXITY_IO    				
            ,(((COMPLEXITY_CPU + COMPLEXITY_IO +0.5)/2) (DECIMAL(6,0))) as COMPLEXITY_Effect            				
            ,CASE				
                WHEN DelayTime is NULL                      THEN '0000 - 0000'				
                WHEN DelayTime < 1.0 or DelayTime is NULL   THEN '0000 - 0001'				
                WHEN DelayTime BETWEEN 1.0 AND 5.0          THEN '0001 - 0005'				
                WHEN DelayTime BETWEEN 5.0 AND 10.0         THEN '0005 - 0010'				
                WHEN DelayTime BETWEEN 10.0 AND 30.0        THEN '0010 - 0030'				
                WHEN DelayTime BETWEEN 30.0 AND 60.0        THEN '0030 - 0060'				
                WHEN DelayTime BETWEEN 60.0 AND 300.0       THEN '0060 - 0300'				
                WHEN DelayTime BETWEEN 300.0 AND 600.0      THEN '0300 - 0600'				
                WHEN DelayTime BETWEEN 600.0 AND 1800.0     THEN '0600 - 1800'				
                WHEN DelayTime BETWEEN 1800.0 AND 3600.0    THEN '1800 - 3600'				
                WHEN DelayTime > 3600.0                     THEN '3600+'				
             END AS DelaySeconds_Class				
            ,CASE				
                WHEN Parse_Time_Secs IS NULL                    THEN '0000 - 0000'				
                WHEN Parse_Time_Secs < 1.0                      THEN '0000 - 0001'				
                WHEN Parse_Time_Secs BETWEEN 1.0 AND 5.0        THEN '0001 - 0005'				
                WHEN Parse_Time_Secs BETWEEN 5.0 AND 10.0       THEN '0005 - 0010'				
                WHEN Parse_Time_Secs BETWEEN 10.0 AND 30.0      THEN '0010 - 0030'				
                WHEN Parse_Time_Secs BETWEEN 30.0 AND 60.0      THEN '0030 - 0060'				
                WHEN Parse_Time_Secs BETWEEN 60.0 AND 300.0     THEN '0060 - 0300'				
                WHEN Parse_Time_Secs BETWEEN 300.0 AND 600.0    THEN '0300 - 0600'				
                WHEN Parse_Time_Secs BETWEEN 600.0 AND 1800.0   THEN '0600 - 1800'				
                WHEN Parse_Time_Secs BETWEEN 1800.0 AND 3600.0  THEN '1800 - 3600'				
                WHEN Parse_Time_Secs > 3600.0                   THEN '3600+'				
             END AS Parse_Time_Class				
            ,CASE				
                WHEN AMPCPUTime IS NULL                        THEN '00000 - 000000'				
		WHEN AMPCPUTime < 1.0                          THEN '00000 - 000001'		
                WHEN AMPCPUTime BETWEEN 1.0 AND 10.0           THEN '00001 - 000010'				
                WHEN AMPCPUTime BETWEEN 10.0 AND 100.0         THEN '00010 - 000100'				
                WHEN AMPCPUTime BETWEEN 100.0 AND 1000.0       THEN '00100 - 001000'				
                WHEN AMPCPUTime BETWEEN 1000.0 AND 10000.0     THEN '01000 - 010000'				
                WHEN AMPCPUTime BETWEEN 10000.0 AND 100000.0   THEN '10000 - 100000'				
                WHEN AMPCPUTime > 100000.0                     THEN '100000+'				
            END AS AMPCPUTime_Class				
            ,CASE				
                WHEN ParserCPUTime IS NULL                     THEN '00000 - 00000'				
                WHEN ParserCPUTime < 1.0                       THEN '00000 - 00001'				
                WHEN ParserCPUTime BETWEEN 1.0 AND 5.0         THEN '00001 - 00005'				
                WHEN ParserCPUTime BETWEEN 5.0 AND 10.0        THEN '00005 - 00010'				
                WHEN ParserCPUTime BETWEEN 10.0 AND 50.0       THEN '00010 - 00050'				
                WHEN ParserCPUTime BETWEEN 50.0 AND 100.0      THEN '00050 - 00100'				
                WHEN ParserCPUTime BETWEEN 100.0 AND 500.0     THEN '00100 - 00500'				
                WHEN ParserCPUTime BETWEEN 500.0 AND 1000.0    THEN '00500 - 01000'				
                WHEN ParserCPUTime BETWEEN 1000.0 AND 5000.0   THEN '01000 - 05000'				
                WHEN ParserCPUTime BETWEEN 5000.0 AND 10000.0  THEN '05000 - 10000'				
                WHEN ParserCPUTime > 10000.0                   THEN '10000+'				
             END AS ParserCPUTime_Class     				
            ,CASE				
                WHEN TotalIOCount IS NULL                 THEN '1e0-1e0'				
                WHEN TotalIOCount < 1e4                   THEN '1e0-1e4'				
                WHEN TotalIOCount BETWEEN 1e4 AND 1e6     THEN '1e4-1e6'				
                WHEN TotalIOCount BETWEEN 1e6 AND 1e8     THEN '1e6-1e8'				
                WHEN TotalIOCount BETWEEN 1e8 AND 1e10    THEN '1e8-1e10'				
                WHEN TotalIOCount > 1e10                  THEN '1e10+'				
             END AS TotalIOCount_Class                  				
            ,CASE				
                WHEN Execution_Time_Secs IS NULL                   THEN '00000 - 000000'				
		WHEN Execution_Time_Secs < 1.0                     THEN '00000 - 000001'		
                WHEN Execution_Time_Secs BETWEEN 1.0 AND 1e1       THEN '00001 - 000010'				
                WHEN Execution_Time_Secs BETWEEN 1e1 AND 1e2       THEN '00010 - 000100'				
                WHEN Execution_Time_Secs BETWEEN 1e2 AND 1e3       THEN '00100 - 001000'				
                WHEN Execution_Time_Secs BETWEEN 1e3 AND 1e4       THEN '01000 - 010000'				
                WHEN Execution_Time_Secs > 1e4                     THEN '10000+'				
            END AS Execution_Time_Class				
            ,CASE				
                WHEN Transfer_Time_Secs IS NULL                   THEN '0000 - 0000'				
                WHEN Transfer_Time_Secs < 1.0                     THEN '0000 - 0001'				
                WHEN Transfer_Time_Secs BETWEEN 1.0 AND 5.0       THEN '0001 - 0005'				
                WHEN Transfer_Time_Secs BETWEEN 5.0 AND 10.0      THEN '0005 - 0010'				
                WHEN Transfer_Time_Secs BETWEEN 10.0 AND 30.0     THEN '0010 - 0030'				
                WHEN Transfer_Time_Secs BETWEEN 30.0 AND 60.0     THEN '0030 - 0060'				
                WHEN Transfer_Time_Secs BETWEEN 60.0 AND 300.0    THEN '0060 - 0300'				
                WHEN Transfer_Time_Secs BETWEEN 300.0 AND 600.0   THEN '0300 - 0600'				
                WHEN Transfer_Time_Secs BETWEEN 600.0 AND 1800.0  THEN '0600 - 1800'				
                WHEN Transfer_Time_Secs BETWEEN 1800.0 AND 3600.0 THEN '1800 - 3600'				
                WHEN Transfer_Time_Secs > 3600.0                  THEN '3600 - 9999'				
            END AS Transfer_Time_Class           				
             				
            ,CASE				
                WHEN TotalIOCount = 0				
                    THEN 'No I/O'				
                WHEN TotalIOCount > 0 AND ReqPhysIO = 0				
                    THEN 'In Memory'				
                WHEN TotalIOCount > 0 AND ReqPhysIO > 0				
                    THEN 'Physical I/O'				
             END AS IO_Optimization				
 				
 /* Cache Miss Rate IOPS.  normal cache miss rate <20%,   set score = 0  for  miss rate < 20%,  increments of 10%, range 0 -80 */  				
            ,case				
                when  SumPhysKioCnt = 0 then 0   				
                when   zeroifnull(SumPhysKioCnt/ nullifzero(SumKio)) <= 0.20 then 0                         /* set score = 0 when less than industry average 20% */				
                when   SumPhysKioCnt > SumKio then 80                                                       /* sometimes get Physical > Logical, set ceiling at 80*/				
                else (cast( 100 * zeroifnull (SumPhysKioCnt/ nullifzero(SumKio)) /10 as  integer) * 10) - 20  /* only count above 20%, round to bin size 10*/				
            end as CacheMissIOPSScore                    				
                    				
 /* Cache Miss Rate KB.  normal cache miss rate <20%,   set score = 0  for  miss rate < 20%,  increments of 10%, range 0 -80 */  				
            ,case 				
                when  SumPhysIO_GB = 0 then 0   				
                when   zeroifnull(SumPhysIO_GB/ nullifzero(SumLogIO_GB)) <= 0.20 then 0                   /* set score = 0 when less than industry average 20% */				
                when   SumPhysIO_GB > SumLogIO_GB then 80                                  /* sometimes get Physical > Logical, set ceiling at 80*/				
                else  (cast( 100 * zeroifnull (SumPhysIO_GB/ nullifzero(SumLogIO_GB)) /10 as  integer) * 10) - 20   /* only count above 20%, round to bin size 10*/				
            end as CacheMissKBScore   				
				
        FROM				
            PDCRINFO.DBQLogTbl_Hst QryLog				
            INNER JOIN				
            Sys_Calendar.CALENDAR QryCal				
                ON QryCal.calendar_date = QryLog.LogDate				
        WHERE				
            --LogDate BETWEEN current_date - 1  AND current_date				
            LogDate BETWEEN current_date - 30 AND current_date - 1				
            --AND Extract (HOUR from StartTime) in (8,9,10)				
            AND StartTime IS NOT NULL				
            AND AMPCPUTime > 0				
            AND QrySecs > 0				
        EXPAND ON QryPeriod AS QPer BY ANCHOR ANCHOR_MINUTE				
----- ----- ----- -----				
)  x1				
--where PctCPU < 1			
)  x2				
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20				
)  x3				
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16				
Order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16				
;				



-------------------------------------

Query 6

/*
CPU Trend Spma AvgCPUBusyPct
CPU Utilization 4-Hour Variable Peak from ResusageSpma (Viewpoint CPU Utilization Method).  
•	Evaluates the percentage of time CPU’s in the collection period that CPU’s were busy processing requests (CPUUServ + CPUUExec 0
•	AvgCPUBusyPct is an average of CPU utilization across the entire system. The premise is that when CPU’s reach 80% busy (i.e., reserve capacity level) the system will likely suffer performance impact.   
•	Evaluates 365 days of history (weekdays only) and the busiest 4-hours of each day (whenever they occur).  Peak periods are contiguous 4-hour periods (not individual hours) and may vary (PeakStart & PeakEnd displayed in result) depending on utilization.  
•	Peak periods may start on one day and end in the next – peak periods are always recorded on the day in which they end.
•	Simple linear regression is used to determine trend line with slope & intercept.
•	The slope of the trend line is used to extend anticipated usage 365 days into the future or when utilization is forecasted to exceed 100% (whichever comes first). 
•	21 day Moving Average is included to emphasize recent activity in addition to the longer trend (typically 21 business days in a calendar month).
•	Reserve Capacity is set at 80% (workload performance will likely be impacted when CPU’s exceed 80% utilization).
•	Reserve Horizon represents the (future) point in time at which utilization is expected to exceed 80%.
•	Slope is the daily percentage increase/decrease in utilization of the trend line (positive = increasing utilization, negative = decreasing utilization). 
•	SQL uses UNION to combine historical trend with future forecast – identical changes typically must be made to both SQL statements in UNION.
•	


*/

LOCK ROW FOR ACCESS 
SELECT
'SiteId' as SiteID /* Enter the Customer SiteID */
,Current_Date (format'YYYY-MM-DD') (CHAR(10)) as "Report Date"
,TheDate(format'YYYY-MM-DD') (CHAR(10)) as "Log Date"
,PeakStart ||':00:00' as "Peak Start"
,PeakEnd ||':00:00' as "Peak End"
,AvgCPUPct (DECIMAL(18,4)) as "Avg CPU Pct"
,CASE 
 WHEN Period_Number < 21 THEN NULL 
 WHEN AvgCPUPct IS NULL THEN NULL 
 ELSE MovingAvg END (DECIMAL(18,4)) AS "Moving Avg"
,Trend (DECIMAL(18,4)) as Trend
,ReserveX 
,CASE WHEN Trend >= ReserveX THEN Trend ELSE NULL END (DECIMAL(18,4)) AS "Reserve Horizon" 
,SlopeX (DECIMAL(18,4)) as SlopeX
FROM 
(
SELECT
 SiteID
,COUNT(*) OVER (ORDER BY calendar_date ROWS UNBOUNDED PRECEDING ) AS Period_Number
,Calendar_Date AS TheDate
,VPeakAvgCPUPct AS AvgCPUPct
,AVG(VPeakAvgCPUPct ) OVER (ORDER BY  Calendar_Date ROWS 21 PRECEDING) AS MovingAvg
,CASE WHEN VPeakAvgCPUPct IS NOT NULL THEN 1 ELSE 0 END AS CountX
,SUM(CountX) OVER ( ) AS CountAll
,ForecastX AS Trend
,80 AS ReserveX /* Enter the Reserve CPU percentage (in whole numbers).  Typically 80 - 90. */
,SlopeX
,PeakStart
,PeakEnd

FROM 
(
SELECT
 SiteID
,Period_Number
,a4.Month_Of_Calendar
,a4.TheDate
,NULL (DECIMAL(38,6)) AS VPeakAvgCPUPct 
,a4.TrendX
,a4.SlopeX
,NULL (CHAR(13)) as PeakStart
,NULL (CHAR(13)) as PeakEnd
,a4.TheHour
,c2.calendar_date
,COUNT(*) OVER (ORDER BY c2.calendar_date ROWS UNBOUNDED PRECEDING ) AS SequenceNbr
,a4.TrendX + (a4.SlopeX * SequenceNbr) AS ForecastX
,VPeakAvgCPUPct AS ExtVPeakAvgCPUPct 

FROM 
(SELECT
 SiteID
,Period_Number
,Month_Of_Calendar
,TheDate
,VPeakAvgCPUPct 
,CAST(REGR_INTERCEPT(VPeakAvgCPUPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6))
+ Period_Number * CAST((REGR_SLOPE(VPeakAvgCPUPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,6)) AS TrendX
,CAST(REGR_SLOPE(VPeakAvgCPUPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6)) AS SlopeX
,PeakStart
,PeakEnd
,TheHour
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,VPeakAvgCPUPct 
,ROW_NUMBER() OVER (ORDER BY TheDate) AS Period_Number
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,HourlyAvgCPUPct
,VPeakAvgCPUPct 
FROM (
SELECT
 'SiteID' as SiteID
,TheDate
,c1.Month_Of_Calendar
,(TheTime/10000 (SMALLINT)) AS TheHour
,( TheDate (DATE, FORMAT 'YYYY-MM-DD'))||' '||TRIM(TheHour (FORMAT '99')) AS PeakEnd
,AVG((((s1.CPUUServ (DECIMAL(38,6))) + s1.CPUUExec)/NULLIFZERO((s1.NCPUs (DECIMAL(38,6)))
))/(s1.Secs (DECIMAL(38,6)))
) AS HourlyAvgCPUPct
,AVG((HourlyAvgCPUPct (DECIMAL(38,6)))
) OVER (ORDER BY  TheDate ,TheHour ROWS 3 PRECEDING) AS VPeakAvgCPUPct /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
,MIN((TheDate (DATE, FORMAT 'YYYY-MM-DD')) ||' '||TRIM(TheHour (FORMAT '99'))) OVER  (ORDER BY  TheDate ,TheHour ROWS 3 PRECEDING) AS PeakStart /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
--FROM DBC.ResUsageSPMA s1,
FROM PDCRINFO.ResUsageSPMA_hst s1,
sys_calendar.CALENDAR c1
WHERE  c1.calendar_date= s1.TheDate
AND s1.vproc1 > 0
AND c1.day_of_week IN (2,3,4,5,6) 
AND s1.TheDate BETWEEN (CURRENT_DATE - 365) AND CURRENT_DATE  /* Enter number of days for history.  Typically 365  */
GROUP BY 1,2,3,4) a1
QUALIFY ROW_NUMBER () OVER (PARTITION BY TheDate ORDER BY VPeakAvgCPUPct  DESC) = 1) a2
) a3
QUALIFY ROW_NUMBER () OVER (ORDER BY TheDate  DESC) = 1
) a4,
sys_calendar.CALENDAR c2
WHERE  c2.calendar_date BETWEEN a4.TheDate+1 AND a4.TheDate + (365*2) /* Enter number of days for future forecast.  Typically 365*2  */
AND c2.day_of_week IN (2,3,4,5,6)

UNION

SELECT
 SiteID
,Period_Number 
,Month_Of_Calendar
,TheDate
,VPeakAvgCPUPct 
,CAST(REGR_INTERCEPT(VPeakAvgCPUPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6))
+ Period_Number * CAST((REGR_SLOPE(VPeakAvgCPUPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,6)) AS TrendX
,CAST(REGR_SLOPE(VPeakAvgCPUPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6)) AS SlopeX
,PeakStart
,PeakEnd
,TheHour
,TheDate
,0
,TrendX
,VPeakAvgCPUPct AS ExtVPeakAvgCPUPct 
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,VPeakAvgCPUPct 
,ROW_NUMBER() OVER (ORDER BY TheDate) AS Period_Number
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,HourlyAvgCPUPct
,VPeakAvgCPUPct 
FROM (
SELECT
'SiteID' as SiteID
,TheDate
,c1.Month_Of_Calendar
,(TheTime/10000 (SMALLINT)) AS TheHour
,( TheDate (DATE, FORMAT 'YYYY-MM-DD'))||' '||TRIM(TheHour (FORMAT '99')) AS PeakEnd
,AVG((((s1.CPUUServ (DECIMAL(38,6))) + s1.CPUUExec)/NULLIFZERO((s1.NCPUs (DECIMAL(38,6)))
))/(s1.Secs (DECIMAL(38,6)))
) AS HourlyAvgCPUPct
,AVG((HourlyAvgCPUPct (DECIMAL(38,6)))
) OVER (ORDER BY  TheDate ,TheHour ROWS 3 PRECEDING) AS VPeakAvgCPUPct /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
,MIN((TheDate (DATE, FORMAT 'YYYY-MM-DD')) ||' '||TRIM(TheHour (FORMAT '99'))) OVER  (ORDER BY  TheDate ,TheHour ROWS 3 PRECEDING) AS PeakStart /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
--FROM DBC.ResUsageSPMA s1,
FROM PDCRINFO.ResUsageSPMA_hst s1,
sys_calendar.CALENDAR c1
WHERE  c1.calendar_date= s1.TheDate
AND s1.vproc1 > 0
AND c1.day_of_week IN (2,3,4,5,6) 
AND s1.TheDate BETWEEN (CURRENT_DATE - 365) AND CURRENT_DATE  /* Enter number of days for history.  Typically 365  */
GROUP BY 1,2,3,4) a1
QUALIFY ROW_NUMBER () OVER (PARTITION BY TheDate ORDER BY VPeakAvgCPUPct  DESC) = 1) a2
) a3
) a4
WHERE ForecastX < 100
) a5
ORDER BY 1,2,3;

Query 7

/*
I/O Trend Sldv 80Pct
DBC Option (uses PDCRINFO.ResusageSldv_hst)
Current-365 days history & Current+365 days forecast

I/O Utilization 4-Hour Variable Peak from ResusageSldv (Viewpoint I/O Utilization Method).  
•	Evaluates the percentage of time in the collection period that devices were busy processing I/O requests (ldvOutReqTime) for ldvreads > 0 & ldvtype = 'DISK' (no need for Archie or I/O capacity estimates).  
•	IOBusyPct assessment is at 80th percentile of busiest drives (not an average). 80% of devices are less busy, 20% of drives are more busy.  The premise is that when 20% of drives reach 80% busy (i.e., reserve capacity level) the system will likely suffer performance impact.   
•	Evaluates 365 days of history (weekdays only) and the busiest 4-hours of each day (whenever they occur).  Peak periods are contiguous 4-hour periods (not individual hours) and may vary (PeakStart & PeakEnd displayed in result) depending on utilization.  
•	Peak periods may start on one day and end in the next – peak periods are always recorded on the day in which they end.
•	Simple linear regression is used to determine trend line with slope & intercept.
•	The slope of the trend line is used to extend anticipated usage 365 days into the future or when utilization is forecasted to exceed 100% (whichever comes first). 
•	21 day Moving Average is included to emphasize recent activity in addition to the longer trend (typically 21 business days in a calendar month).
•	Reserve Capacity is set at 80% (workload performance will likely be impacted when busiest 20% of drives exceed 80% utilization).
•	Reserve Horizon represents the (future) point in time at which utilization is expected to exceed 80%.
•	Slope is the daily percentage increase/decrease in utilization of the trend line (positive = increasing utilization, negative = decreasing utilization). 
•	SQL uses UNION to combine historical trend with future forecast – identical changes typically must be made to both SQL statements in UNION.

*/

----- SQL ----- ----- ----- ----- -----*/
LOCK ROW FOR ACCESS 
SELECT
 SiteID as SiteID  /* Enter the Customer SiteID */
,current_date (format'YYYY-MM-DD') (CHAR(10)) as "Report Date"
,TheDate (format'YYYY-MM-DD') (CHAR(10)) as "Log Date"
,PeakStart ||':00:00' as "Peak Start"
,PeakEnd ||':00:00' as "Peak End"
,AvgIOPct (DECIMAL(18,4)) as "Avg I/O Pct"
,CASE 
 WHEN Period_Number < 21 THEN NULL 
 WHEN AvgIOPct IS NULL THEN NULL 
 ELSE MovingAvg END (DECIMAL(18,4)) AS "Moving Avg"
,Trend (DECIMAL(18,4)) as Trend
,ReserveX 
,CASE WHEN Trend >= ReserveX THEN Trend ELSE NULL END (DECIMAL(18,4)) AS "Reserve Horizon" 
,SlopeX (DECIMAL(18,4))as SlopeX

FROM 
(
SELECT
--(a6)
 SiteID
,COUNT(*) OVER (ORDER BY calendar_date ROWS UNBOUNDED PRECEDING ) AS Period_Number
,Month_Of_Calendar
,Calendar_Date AS TheDate
,PeakStart
,PeakEnd
,VPeakAvgIOPct AS AvgIOPct
,AVG(VPeakAvgIOPct ) OVER (ORDER BY  Calendar_Date ROWS 21 PRECEDING) AS MovingAvg

,CASE WHEN VPeakAvgIOPct IS NOT NULL THEN 1 ELSE 0 END AS CountX
,SUM(CountX) OVER ( ) AS CountAll
,MIN(Trend*CountX) OVER ( ) AS MINTrend
,MAX(Trend*CountX) OVER ( ) AS MAXTrend 
,ForecastX AS Trend
,80 AS ReserveX  /* Enter the Reserve I/O percentage (in whole numbers).  Typically 80 - 90. */
,MIN(Month_Of_Calendar) OVER () AS MinMonthAll
,MAX(Month_Of_Calendar) OVER () AS MAXMonthAll
,(MAXMonthAll - MINMOnthAll) + 1 AS MonthsNumber
,SlopeX

FROM (
--UNION-1 Forecast
SELECT
 a5.SiteID
,a5.Period_Number 
,c2.Month_Of_Calendar
,a5.TheDate
,NULL (DECIMAL(38,6)) AS VPeakAvgIOPct 
,a5.TrendX
,a5.SlopeX
,NULL (CHAR(13)) as PeakStart
,NULL (CHAR(13)) as PeakEnd
,a5.TheHour
,c2.calendar_date
,COUNT(*) OVER (ORDER BY c2.calendar_date ROWS UNBOUNDED PRECEDING ) AS SequenceNbr
,a5.TrendX + (a5.SlopeX * SequenceNbr) AS ForecastX
,VPeakAvgIOPct AS ExtVPeakAvgIOPct 
,COUNT(*) OVER ( ) AS CountAll

FROM 
(
--(a5)
SELECT
 SiteID
,Period_Number 
,Month_Of_Calendar
,TheDate
,VPeakAvgIOPct 
,CAST(REGR_INTERCEPT(VPeakAvgIOPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6))
+ Period_Number * CAST((REGR_SLOPE(VPeakAvgIOPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,6)) AS TrendX
,CAST(REGR_SLOPE(VPeakAvgIOPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6)) AS SlopeX
,PeakStart
,PeakEnd
,TheHour
FROM (
--(a4)
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,VPeakAvgIOPct 
,ROW_NUMBER() OVER (ORDER BY TheDate) AS Period_Number
FROM (
--(a3)
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,HourlyAvgIOPct
,VPeakAvgIOPct 
FROM (
--(a2)
SELECT
 SiteID
,TheDate
,Month_Of_Calendar
,TheHour
,( TheDate (DATE, FORMAT 'YYYY-MM-DD'))||' '||TRIM(TheHour (FORMAT '99')) AS PeakEnd
--,COUNT(*) AS LUNCount
,AVG(MinDiskPct) AS HourlyAvgIOPct
,AVG(HourlyAvgIOPct) OVER (ORDER BY  SiteID, TheDate ,TheHour ROWS 3 PRECEDING) AS VPeakAvgIOPct  /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
,MIN((TheDate (DATE, FORMAT 'YYYY-MM-DD')) ||' '||TRIM(TheHour (FORMAT '99'))) OVER  (ORDER BY  SiteID, TheDate ,TheHour ROWS 3 PRECEDING) AS PeakStart  /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
from
(
Select 
--(CC) Identify 1st device with NumDiskPct >=0.80, eliminate all others
 'SiteID' as SiteID
,Month_Of_Calendar
,TheDate
,TheHour
,TheMinute
,MIN(DiskPct) as MinDiskPct
,AVG(DiskPct (DECIMAL(18,4))) as AvgDiskPct
,MAX(DiskPct) as MaxDiskPct
,MAX(TotalCount2) as TotalActiveDevices
,MIN(TotalCount3 ) - 1 as CountDevicesBelow80th
,Count(*) as CountDevicesAbove80th
,(CountDevicesBelow80th(DECIMAL(18,4)))/TotalActiveDevices as PctDevicesBelow80th
--,MIN(NumDiskPct) as PctDevicesBelow80th
,1-PctDevicesBelow80th as PctDevicesAbove80th
from (
--(BB) Reduce result to 20% most busy devices (i.e., 1st device with NumDiskPct >=0.80
Select 
TheDate
,TheHour
,Month_Of_Calendar
,TheMinute
,AvgDiskPct2
,NodeID
,CtlID
,LdvID
,DiskPct
,Count(*) as TotalCount
,SUM(TotalCount) over (partition by TheDate, TheHour, TheMinute) as TotalCount2
,(SUM(TotalCount) over (partition by TheDate, TheHour, TheMinute order by TheDate, TheHour, TheMinute, DiskPct, NodeID, CtlID, LdvID 
 ROWS UNBOUNDED PRECEDING))  as TotalCount3
,(TotalCount3 (DECIMAL(18,4)))/(TotalCount2 (DECIMAL(18,4))) as NumDiskPct
FROM (
--(AA) SELECT qualifying data from Sldv
--ldvreads > 0
--ldvtype = 'DISK'
select
 s1.TheDate
,c1.Month_Of_Calendar
,EXTRACT(HOUR from s1.TheTime) as TheHour
,EXTRACT(MINUTE from s1.TheTime) as TheMinute
,s1.NodeID
,s1.CtlID
,s1.LdvID
,(cast(s1.ldvOutReqTime as decimal(18,4))/secs) as DiskPct  
,AVG(DiskPct) over (partition by TheDate, TheHour, TheMinute) as AvgDiskPct2
--from dbc.ResUsageSldv s1,
from PDCRINFO.ResUsageSldv_hst s1,
sys_calendar.CALENDAR c1
WHERE  c1.calendar_date= s1.TheDate
AND c1.day_of_week IN (2,3,4,5,6) 
and s1.LdvType='DISK'
AND s1.ldvreads > 0
AND s1.TheDate BETWEEN (Current_Date - 365) AND Current_Date  /* Enter number of days for history.  Typically 365  */
) as AA
Qualify NumDiskPct >= .80
group by TheDate, Month_Of_Calendar, TheHour, TheMinute, AvgDiskPct2, NodeID, CtlID, LdvID, DiskPct
--Order by TheDate, TheHour, TheMinute, AvgDiskPct2, TotalCount3
) as BB
group by 1,2,3,4,5
) as CC
GROUP BY 1,2,3,4
) a2
QUALIFY ROW_NUMBER () OVER (PARTITION BY TheDate ORDER BY VPeakAvgIOPct  DESC) = 1
) a3
) a4
QUALIFY ROW_NUMBER () OVER (ORDER BY TheDate  DESC) = 1
) a5,
sys_calendar.CALENDAR c2
WHERE  c2.calendar_date BETWEEN a5.TheDate+1 AND a5.TheDate + (365*2)  /* Enter number of days for future forecast.  Typically 365*2  */
AND c2.day_of_week IN (2,3,4,5,6)
----- ----- -----
----- ----- -----
UNION
----- ----- -----
----- ----- -----
SELECT
--UNION-2 Historical Trend Line
 SiteID
,Period_Number 
,Month_Of_Calendar
,TheDate
,VPeakAvgIOPct 
,CAST(REGR_INTERCEPT(VPeakAvgIOPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6))
+ Period_Number * CAST((REGR_SLOPE(VPeakAvgIOPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,6)) AS TrendX
,CAST(REGR_SLOPE(VPeakAvgIOPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6)) AS SlopeX
,PeakStart
,PeakEnd
,TheHour
,TheDate
,0 as SequenceNbr
,TrendX AS ForecastX
,VPeakAvgIOPct AS ExtVPeakAvgIOPct 
,COUNT(*) OVER ( ) AS CountAll
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,VPeakAvgIOPct 
,ROW_NUMBER() OVER (ORDER BY TheDate) AS Period_Number
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,HourlyAvgIOPct
,VPeakAvgIOPct 
FROM (
SELECT
 SiteID
,TheDate
,Month_Of_Calendar
,TheHour
,( TheDate (DATE, FORMAT 'YYYY-MM-DD'))||' '||TRIM(TheHour (FORMAT '99')) AS PeakEnd
--,COUNT(*) AS LUNCount
,AVG(MinDiskPct) AS HourlyAvgIOPct
,AVG(HourlyAvgIOPct) OVER (ORDER BY  SiteID, TheDate ,TheHour ROWS 3 PRECEDING) AS VPeakAvgIOPct /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
,MIN((TheDate (DATE, FORMAT 'YYYY-MM-DD')) ||' '||TRIM(TheHour (FORMAT '99'))) OVER  (ORDER BY  SiteID, TheDate ,TheHour ROWS 3 PRECEDING) AS PeakStart /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
from
(
Select 
--(CC) Identify 1st device with NumDiskPct >=0.80, eliminate all others
 'SiteID' as SiteID
,Month_Of_Calendar
,TheDate
,TheHour
,TheMinute
,MIN(DiskPct) as MinDiskPct
,AVG(DiskPct (DECIMAL(18,4))) as AvgDiskPct
,MAX(DiskPct) as MaxDiskPct
,MAX(TotalCount2) as TotalActiveDevices
,MIN(TotalCount3 ) - 1 as CountDevicesBelow80th
,Count(*) as CountDevicesAbove80th
,(CountDevicesBelow80th(DECIMAL(18,4)))/TotalActiveDevices as PctDevicesBelow80th
--,MIN(NumDiskPct) as PctDevicesBelow80th
,1-PctDevicesBelow80th as PctDevicesAbove80th
from (
--(BB) Reduce result to 20% most busy devices (i.e., 1st device with NumDiskPct >=0.80
Select 
TheDate
,TheHour
,Month_Of_Calendar
,TheMinute
,AvgDiskPct2
,NodeID
,CtlID
,LdvID
,DiskPct
,Count(*) as TotalCount
,SUM(TotalCount) over (partition by TheDate, TheHour, TheMinute) as TotalCount2
,(SUM(TotalCount) over (partition by TheDate, TheHour, TheMinute order by TheDate, TheHour, TheMinute, DiskPct, NodeID, CtlID, LdvID 
 ROWS UNBOUNDED PRECEDING))  as TotalCount3
,(TotalCount3 (DECIMAL(18,4)))/(TotalCount2 (DECIMAL(18,4))) as NumDiskPct
FROM (
--(AA) SELECT qualifying data from Sldv
--ldvreads > 0
--ldvtype = 'DISK'
select
 s1.TheDate
,c1.Month_Of_Calendar
,EXTRACT(HOUR from s1.TheTime) as TheHour
,EXTRACT(MINUTE from s1.TheTime) as TheMinute
,s1.NodeID
,s1.CtlID
,s1.LdvID
,(cast(s1.ldvOutReqTime as decimal(18,4))/secs) as DiskPct  
,AVG(DiskPct) over (partition by TheDate, TheHour, TheMinute) as AvgDiskPct2
--from dbc.ResUsageSldv s1,
from PDCRINFO.ResUsageSldv_hst s1,
sys_calendar.CALENDAR c1
WHERE  c1.calendar_date= s1.TheDate
AND c1.day_of_week IN (2,3,4,5,6) 
and s1.LdvType='DISK'
AND s1.ldvreads > 0
AND s1.TheDate BETWEEN (Current_Date - 365) AND Current_Date  /* Enter number of days for history.  Typically 365  */
) as AA
Qualify NumDiskPct >= .80
group by TheDate, Month_Of_Calendar, TheHour, TheMinute, AvgDiskPct2, NodeID, CtlID, LdvID, DiskPct
--Order by TheDate, TheHour, TheMinute, AvgDiskPct2, TotalCount3
) as BB
group by 1,2,3,4,5
) as CC
GROUP BY 1,2,3,4
) a2
QUALIFY ROW_NUMBER () OVER (PARTITION BY TheDate ORDER BY VPeakAvgIOPct  DESC) = 1
) a3
) a4
) a6
WHERE ForecastX < 100  /* Enter percentage of forecast capacity (in whole numbers).  Typically 100  */
) a7
ORDER BY 1,2,3;


Query 8

/*

Storage Trend DatabaseSpace AvgCurrentPermPct
PDCR Option (uses PDCRINFO.DatabaseSpace_Hst)
Version 11 (2019-05-07)
Current-365 days history & Current+365 days forecast

Storage Trend & Forecast  
•	Simple linear regression is used to determine trend line with slope & intercept.
•	The slope of the trend line is used to extend anticipated usage 365 days into the future or when utilization is forecasted to exceed 100% (whichever comes first). 
•	21 day Moving Average is included to emphasize recent activity in addition to the longer trend (typically 21 business days in a calendar month).
•	Reserve Capacity is set at 80% (workload performance will likely be impacted when CPU’s exceed 80% utilization).
•	Reserve Horizon represents the (future) point in time at which utilization is expected to exceed 80%.
•	Slope is the daily percentage increase/decrease in utilization of the trend line (positive = increasing utilization, negative = decreasing utilization). 
•	SQL uses UNION to combine historical trend with future forecast – identical changes typically must be made to both SQL statements in UNION.
*/

----- SQL ----- ----- ----- ----- -----*/
LOCK ROW FOR ACCESS 
SELECT
 'SiteID'  /* Enter the Customer SiteID */
,Current_Date (format'YYYY-MM-DD') (CHAR(10)) as "Report Date"
,TheDate(format'YYYY-MM-DD') (CHAR(10)) as "Log Date"
,TotalMaxPerm as "Total Max Perm"
,TotalCurPerm as "Total Current Perm"
,TotalPeakPerm as "Total Peak Perm"
,TotalAvailPerm as "Total Available Perm"
,TotalCurPct (DECIMAL(18,4)) as "Total Current Pct"
,TotalAvailPct (DECIMAL(18,4)) as "Total Available Pct" 
,CASE 
 WHEN Period_Number < 21 THEN NULL 
 WHEN TotalCurPct IS NULL THEN NULL 
 ELSE MovingAvg END (DECIMAL(18,4)) AS "Moving Avg"
,Trend (DECIMAL(18,4)) as Trend
,ReserveX (DECIMAL(18,4)) as ReserveX
,CASE WHEN Trend >= ReserveX THEN Trend ELSE NULL END (DECIMAL(18,4)) AS "Reserve Horizon" 
,SlopeX (DECIMAL(18,4)) as SlopeX

FROM
(
SELECT
 SiteID
,COUNT(*) OVER (ORDER BY calendar_date ROWS UNBOUNDED PRECEDING ) AS Period_Number
,Calendar_Date AS TheDate
,TotalMaxPerm
,TotalCurPerm
,TotalPeakPerm
,TotalAvailPerm
,TotalCurPct
,TotalAvailPct
,AVG(TotalCurPct) OVER (ORDER BY  Calendar_Date ROWS 21 PRECEDING) AS MovingAvg
,TrendX
,SlopeX
,70 AS ReserveX /* Enter the amount of the storage reserve threshold – typically 65 to 80 */
,ForecastX
,ForecastX AS Trend
FROM 
(
SELECT
   SiteID
  ,c2.calendar_date
  ,NULL (BIGINT) as TotalMaxPerm
  ,NULL (BIGINT) as TotalCurPerm
  ,NULL (BIGINT) as TotalPeakPerm
  ,NULL (BIGINT) as TotalAvailPerm
  ,NULL (DECIMAL(18,4)) as TotalCurPct
  ,NULL (DECIMAL(18,4)) as TotalAvailPct
  ,COUNT(*) OVER (ORDER BY c2.calendar_date ROWS UNBOUNDED PRECEDING ) AS SequenceNbr
  ,TrendX
  ,SlopeX
  ,TrendX + (SlopeX * SequenceNbr) AS ForecastX

FROM 
(
SELECT
   SiteID
  ,LogDate
  ,TotalMaxPerm
  ,TotalCurPerm
  ,TotalPeakPerm
  ,TotalAvailPerm
  ,TotalCurPct
  ,TotalAvailPct
  ,Period_Number
  ,CAST(REGR_INTERCEPT(TotalCurPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,8))
+ Period_Number * CAST((REGR_SLOPE(TotalCurPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,8)) AS TrendX
,CAST(REGR_SLOPE(TotalCurPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,8)) AS SlopeX
FROM (
SELECT
   SiteID
  ,LogDate
  ,TotalMaxPerm
  ,TotalCurPerm
  ,TotalPeakPerm
  ,TotalAvailPerm
  ,TotalCurPct
  ,TotalAvailPct
  ,ROW_NUMBER() OVER (ORDER BY LogDate) AS Period_Number
FROM (
SELECT
  'SiteID' as SiteID
  ,LogDate
  ,SUM(MAXPERM)                      AS TotalMaxPerm
  ,SUM(CURRENTPERM)                  AS TotalCurPerm
  ,SUM(PEAKPERM)                     AS TotalPeakPerm
  ,TotalMaxPerm-TotalCurPerm         AS TotalAvailPerm
  ,TotalCurPerm/(TotalMaxPerm (DECIMAL(38,4))) * 100   AS TotalCurPct
  ,TotalAvailPerm/(TotalMaxPerm (DECIMAL(38,4)))* 100 AS TotalAvailPct
  --FROM  ss160000.DatabaseSpace s1,
  FROM  PDCRINFO.DatabaseSpace_Hst s1,
      sys_calendar.CALENDAR c1
  WHERE  c1.calendar_date= s1.LogDate
    AND c1.day_of_week IN (2,3,4,5,6) 
    AND s1.Logdate BETWEEN Current_Date - 365 AND Current_Date /* Enter the number days history */
  Group by 1,2 
) a1
) a2
QUALIFY ROW_NUMBER () OVER (ORDER BY LogDate  DESC) = 1
) a3,
sys_calendar.CALENDAR c2
WHERE  c2.calendar_date BETWEEN a3.LogDate+1 AND (a3.LogDate + (365*2)) /* Enter the number days future forecast */
AND c2.day_of_week IN (2,3,4,5,6)

UNION

SELECT
   SiteID
  ,LogDate
  ,TotalMaxPerm
  ,TotalCurPerm
  ,TotalPeakPerm
  ,TotalAvailPerm
  ,TotalCurPct
  ,TotalAvailPct
  ,Period_Number
  ,CAST(REGR_INTERCEPT(TotalCurPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,8))
   + Period_Number * CAST((REGR_SLOPE(TotalCurPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,8)) AS TrendX
  ,CAST(REGR_SLOPE(TotalCurPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,8)) AS SlopeX
  ,TrendX as ForecastX 
FROM (
SELECT
   SiteID
  ,LogDate
  ,TotalMaxPerm
  ,TotalCurPerm
  ,TotalPeakPerm
  ,TotalAvailPerm
  ,TotalCurPct
  ,TotalAvailPct
  ,ROW_NUMBER() OVER (ORDER BY LogDate) AS Period_Number
FROM (
SELECT
  'SiteID' as SiteID 
  ,LogDate
  ,SUM(MAXPERM)                      AS TotalMaxPerm
  ,SUM(CURRENTPERM)                  AS TotalCurPerm
  ,SUM(PEAKPERM)                     AS TotalPeakPerm
  ,TotalMaxPerm-TotalCurPerm         AS TotalAvailPerm
  ,TotalCurPerm/(TotalMaxPerm (DECIMAL(38,4))) * 100   AS TotalCurPct
  ,TotalAvailPerm/(TotalMaxPerm (DECIMAL(38,4)))* 100 AS TotalAvailPct
  --FROM  ss160000.DatabaseSpace s1,
  FROM  PDCRINFO.DatabaseSpace_Hst s1,
      sys_calendar.CALENDAR c1
  WHERE  c1.calendar_date= s1.LogDate
    AND c1.day_of_week IN (2,3,4,5,6) 
    AND s1.Logdate BETWEEN Current_Date - 365 AND Current_Date /* Enter the number days history */
  Group by 1,2 
) a1
) a2
) a3
) a4
WHERE ForecastX < 100 /* Enter the percentage of storage capacity (whole number).  Typically 100.  */
order by 1,2,3;


-------------------------------

Query 9

SELECT
'SiteID' --<-- enter customer SiteID here
,TheDate (FORMAT 'YYYY-MM-DD') AS "Log Date"
,Extract(Hour from TheTime) AS "Log Hour"
,Extract(Minute from TheTime) AS "Log Minute"
,TheTime AS "Log Time"
,cast((thedate(format'YYYY-MM-DD'))||' '||cast(thetime as char(2))||':'||cast(((extract(minute from TheTime))(format'99')) as char(2))||':00' as timestamp(0)) as "Sys Time"  
,COUNT(distinct NodeID) NodeCount
,SUM(s1.Secs) SecondCount
,AVG((((s1.CPUUServ (DECIMAL(38,6))) + s1.CPUUExec)/NULLIFZERO((s1.NCPUs (DECIMAL(38,6)))))/(s1.Secs (DECIMAL(38,6)))) AS AvgCPUPct
,SUM(FilePreReadKB) as "File Pre Read KB"
,SUM(FileAcqReadKB) as "File Acq Read KB"
,SUM(FileWriteKB)   as "File Write KB"
FROM PDCRINFO.ResUsageSpma_Hst s1
where TheDate between (Current_Date - 365) AND Current_Date 
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6;



/*
SELECT
'TDCLOUD14TD07'  (VARCHAR(30)) as SiteID --<-- enter customer SiteID here
,TheDate (FORMAT 'YYYY-MM-DD') as "Log Date"
,Extract(Hour from TheTime) AS "Log Hour"
,Extract(Minute from TheTime) AS "Log Minute"
,TheTime as "Log Time"
,cast(cast((thedate(format'YYYY-MM-DD'))||' '||cast(thetime as char(2))||':'||cast(((extract(minute from TheTime))/10 (format'9')) as char(1))||'0:00' as timestamp(0)) as CHAR(25)) as "Log TimeStamp"
,MAX(NodeType) as "Node Type"
,PM_COD_CPU as "PM COD CPU" 
,PM_COD_IO as "PM COD IO"
,WM_COD_CPU as "WM COD CPU" 
,WM_COD_IO as "WM COD IO"
,TIER_FACTOR as "Tier Factor"
,NCPUs as "Number CPUs" 
,COUNT(distinct NodeID) as "Node Count"
,SUM(s1.Secs)/"Node Count" as "Collection Period Seconds"
,AVG((((s1.CPUUServ (DECIMAL(38,6))) + s1.CPUUExec)/NULLIFZERO((s1.NCPUs (DECIMAL(38,6)))))/(s1.Secs (DECIMAL(38,6)))) AS "Avg CPU Pct"
,SUM(FilePreReads) as "File Pre-Reads" 
,SUM(FileAcqReads) as "File Acq-Reads" 
,SUM(FileWrites) as "File Writes" 
,SUM(FilePreReadKB) as "File Pre-Read KB"
,SUM(FileAcqReadKB) as "File Acq-Read KB"
,SUM(FileWriteKB)   as "File Write KB"
FROM PDCRINFO.ResUsageSPMA_hst s1
where TheDate between (Current_Date - 365) AND Current_Date 
group by 1,2,3,4,5,6, 8,9,10,11,12,13
--order by 1,2,3,4,5
;

*/

----------------------------------------------

Query 10


/* TDWM Summary */



locking row for access

select

    CAST(a.StartColTime AS DATE)         AS LogDate

   ,EXTRACT(YEAR FROM LogDate)           AS Yr

   ,LogDate (FORMAT 'MMM') (CHAR(3))     AS Mnth

   ,EXTRACT(DAY FROM LogDate)            AS DayOfMonth

   ,LogDate (format 'E3')  (CHAR(10))    AS DayOfWeek

   ,EXTRACT(HOUR FROM a.StartColTime)    AS LogHour

   ,(EXTRACT (MINUTE FROM a.StartColTime)/10)*10 AS Log10Minute

   ,a.CollectTimeStamp

   ,a.WDID

   ,wd.WDName

   ,a.OpEnvID

   ,op.OpEnvName

   ,a.SysConID

   ,a.StartColTime

   ,a.Arrivals

   ,a.ActiveCount

   ,a.Completions

   ,a.MinRespTime

   ,a.MaxRespTime

   ,a.AvgRespTime

   ,a.MinCPUTime

   ,a.MaxCPUTime

   ,a.AvgCPUTime

   ,a.DelayedCount

   ,a.AvgDelayTime

   ,a.ExceptionAbCount

   ,a.ExceptionMvCount

   ,a.ExceptionCoCount

   ,a.ExceptionCount

   ,a.MetSLGCount

   ,a.AbortCount

   ,a.ErrorCount

   ,a.RejectedCount

   ,a.MovedInCount

   ,a.IntervalDelayCnt

   ,a.DelayedQueries

   ,a.OtherCount

   ,a.VirtualPartNum

   ,a.AvgIOWaitTime

   ,a.MaxIOWaitTime

   ,a.AvgOtherWaitTime

   ,a.MaxOtherWaitTime

   ,a.AvgCPURunDelay

   ,a.MaxCPURunDelay

   ,a.AvgSeqRespTime

   ,a.MaxSeqRespTime

   ,a.AvgLogicalIO

   ,a.MaxLogicalIO

   ,a.AvgLogicalKBs

   ,a.MaxLogicalKBs

   ,a.AvgPhysicalIO

   ,a.MaxPhysicalIO

   ,a.AvgPhysicalKBs

   ,a.MaxPhysicalKBs

   ,a.ThrottleBypassed

   ,(a.Completions * a.AVGCPUTime ) / 100.00 (decimal(15,2)) as "CpuTime (Secs)"

   ,((a.DelayedCount * a.AvgDelayTime) / 60)    as "Total DelayTime (mins)"



from DBC.QryLogTdwmSumV  a



   left outer join

   (

        Select

            RuleID as WDID,

            RuleName as WDName

        from

            TDWM.RuleDefs

        where

            (RuleID, Cast(CreateDate * 1000000 + CreateTime as BIGINT)) in

            (

                select

                    RuleID,

                    Max(CAST(CreateDate * 1000000 + CreateTime as BIGINT))

                from

                    TDWM.RuleDefs

                group by 1

            )

        Group by 1,2

   ) wd

     ON  wd.WDID = a.WDId



   left outer join

   (

         Select

              OpEnvId

             ,OpEnvName

         from TDWM.OpEnvs

         group by 1,2

   ) op

      ON op.OpEnvId = a.OpEnvID



where a.StartColTime between current_date - 30 and current_date -1



order by a.StartColTime, wd.WDName

;


---------------------------------------------------

Query 11

Locking Row for Access
  Select
       Logdate AS "Log Date"
      ,extract(hour from a.starttime) as "Log Hour"
      ,Username
     ,WDName
     ,Starttime
     ,a.firststeptime
     ,a.firststepTime
     ,Zeroifnull(DelayTime) as DelayTime
      , (CAST(extract(hour
         From     ((a.firststeptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) * 3600 + extract(minute
         From     ((a.firststeptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) * 60 + extract(second
         From     ((a.firststeptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) AS dec(8,2))) - zeroifnull(cast(delaytime as float)) (float)     as PrsDctnryTime

      , Zeroifnull(CAST(extract(hour
         From     ((a.firstresptime - a.firststepTime) HOUR(2) TO SECOND(6) ) ) * 3600 + extract(minute
         From     ((a.firstresptime - a.firststepTime) HOUR(2) TO SECOND(6) ) ) * 60 + extract(second
         From     ((a.firstresptime - a.firststepTime) HOUR(2) TO SECOND(6) ) ) AS INTEGER) )  as QryRespTime

       , Zeroifnull(CAST(extract(hour
         From     ((a.firstresptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) * 3600 + extract(minute
         From     ((a.firstresptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) * 60 + extract(second
         From     ((a.firstresptime - a.StartTime) HOUR(2) TO SECOND(6) ) ) AS INTEGER) )  as TotalTime
       ,count(*) As NoOfQueries
       from  PDCRINFO.DBQLogTbl_Hst a
      
       Where  DelayTime > 0
       AND a.Logdate between current_date - 30 and current_date - 1
       Group By 1,2,3,4,5,6,7,8,9,10,11;


---------------------------------------------------------------

Query 12

Select				
ResponseT.logdate "Log Date"				
,ResponseT.username "User"				
,Sum(ResponseT.QryCount) QryCount				
,SUM(ResponseT."RT < 1 sec") "RT < 1 sec"				
,SUM(ResponseT."RT 1-5 sec") "RT 1-5 sec"				
,SUM(ResponseT."RT 5-10 sec") "RT 5-10 sec"				
,SUM(ResponseT."RT 10-30 sec") "RT 10-30 sec"				
,SUM(ResponseT."RT 30-60 sec") "RT 30-60 sec"				
,SUM(ResponseT."RT 1-5 min")"RT 1-5 min"				
,SUM(ResponseT."RT 5-10 min")"RT 5-10 min"				
,SUM(ResponseT."RT 10-30 min")"RT 10-30 min"				
,SUM(ResponseT."RT 30-60 min")"RT 30-60 min"				
FROM(				
Select logdate  				
,username   				
,((FirstRespTime - StartTime) HOUR(3) TO SECOND(6)) AS Execution_Time				
,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Execution_Time) * 3600 + EXTRACT(MINUTE FROM Execution_Time) * 60 + EXTRACT(SECOND FROM Execution_Time) AS FLOAT)) AS Execution_Time_Secs				
            				
,CASE				
                WHEN Execution_Time_Secs IS NULL				
                    THEN '00000 - 000000'				
				WHEN Execution_Time_Secs < 1.0
                    THEN '00000 - 000001'				
                WHEN Execution_Time_Secs BETWEEN 1.0 AND 1e1				
                    THEN '00001 - 000010'				
                WHEN Execution_Time_Secs BETWEEN 1e1 AND 1e2				
                    THEN '00010 - 000100'				
                WHEN Execution_Time_Secs BETWEEN 1e2 AND 1e3				
                    THEN '00100 - 001000'				
                WHEN Execution_Time_Secs BETWEEN 1e3 AND 1e4				
                    THEN '01000 - 010000'				
                WHEN Execution_Time_Secs > 1e4				
                    THEN '10000+'				
            END AS Execution_Time_Class				
,Count(*) QryCount				
,SUM(CASE WHEN 	Execution_Time_Secs < 1.0 THEN Execution_Time_Secs END) AS "RT < 1 sec"			
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 1.0 AND 5.0 THEN Execution_Time_Secs END) AS "RT 1-5 sec"			
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 5.0 AND 10.0 THEN Execution_Time_Secs END) AS "RT 5-10 sec"			
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 10.0 AND 30.0 THEN Execution_Time_Secs END) AS "RT 10-30 sec"			
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 30.0 AND 60.0 THEN Execution_Time_Secs END) AS "RT 30-60 sec"			
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 60.0 AND 300.0 THEN Execution_Time_Secs END) AS "RT 1-5 min"			
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 300.0 AND 600.0 THEN Execution_Time_Secs END) AS "RT 5-10 min"			
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 600.0 AND 1800.0 THEN Execution_Time_Secs END) AS "RT 10-30 min"			
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 1800.0 AND 3600.0 THEN Execution_Time_Secs END) AS "RT 30-60 min"			
,SUM(CASE WHEN 	Execution_Time_Secs > 3600.0 THEN Execution_Time_Secs END) AS "RT > 1 hour"			
FROM PDCRINFO.DBQLogTbl_Hst QryLog				
            INNER JOIN				
            Sys_Calendar.CALENDAR QryCal				
                ON QryCal.calendar_date = QryLog.LogDate				
        WHERE				
            LogDate BETWEEN current_date - 30  AND current_date - 1				
            AND StartTime IS NOT NULL				
			Group By 1,2,3,4,5	
)ResponseT				
 Group By 1,2				

 
 
 -----------------------------------------------
 
 Query 13
 
 SELECT 
 A.LogDate as LogDate
,A.USERNAME as MaskedUserName
,count(*) as Request_Count
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -16)))) AS "Collect Statistics"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -17)))) AS "Fallback"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -18)))) AS "SET Table"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -19)))) AS "MULTISET Table"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -20)))) AS "Hashed Table"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -21)))) AS "Trigger"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -22)))) AS "Block Level Compression"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -23)))) AS "Auto Compression"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -24)))) AS "Teradata Columnar"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -25)))) AS "No Primary Index"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -26)))) AS "Row Partitioning"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -27)))) AS "Multi Level Partitioning"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -28)))) AS "8 Byte Partitioning"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -29)))) AS "2 Byte Partitioning"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -30)))) AS "Column Partitioning and Row Partitioning"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -31)))) AS "Column-Partitioned and a NoPI"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -32)))) AS "Column-Partitioned and a PI"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -33)))) AS "Column-Partitioned and a PA"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -34)))) AS "Teradata Temporal"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -35)))) AS "Parameterized Query"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -36)))) AS "Binary LOB"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -37)))) AS "Character LOB"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -38)))) AS "Small LOB"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -39)))) AS "Scalar Sub Query"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -40)))) AS "Statement Info Parcel"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -41)))) AS "User Defined Type"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -42)))) AS "XML Data Type"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -43)))) AS "JSON Data Type"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -44)))) AS "Distinct Data Type"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -45)))) AS "Structure Data Type"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -46)))) AS "Geospatial"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -47)))) AS "3D Geospatial"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -48)))) AS "Period Data Type"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -49)))) AS "Array Data Type"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -50)))) AS "Number Data Type"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -51)))) AS "Teradata Stored Procedure"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -52)))) AS "User Defined Function"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -53)))) AS "External Stored Procedure"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -54)))) AS "Table Function"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -55)))) AS "Table Operator"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -56)))) AS "Foreign Function"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -57)))) AS "Teradata Remote Query"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -58)))) AS "Query Grid"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -59)))) AS "Data Stream Architecture"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -60)))) AS "DBQL SQL Logging"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -61)))) AS "DBQL STEP Logging"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -62)))) AS "DBQL OBJECTS Logging"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -63)))) AS "DBQL EXPLAIN Logging"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -64)))) AS "DBQL XMLPLAN Logging"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -65)))) AS "DBQL STATSUSAGE Logging"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -66)))) AS "DBQL PARAMINFO Logging"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -67)))) AS "DBQL OBJECT USECOUNT Logging"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -68)))) AS "DBQL UTILITYINFO Logging"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -69)))) AS "DBQL THRESHOLD Enabled"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -70)))) AS "Incremental Planning and Execution"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -71)))) AS "Teradata Unity"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -72)))) AS "Autoreparse"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -73)))) AS "Teradata In-Memory"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -74)))) AS "Teradata Pivot"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -75)))) AS "Teradata Unpivot"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -76)))) AS "Redrive"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -77)))) AS "Queryband"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -78)))) AS "Index Analysis"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -79)))) AS "Partition Analysis"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -80)))) AS "Replication"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -81)))) AS "Load Isolation"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -82)))) AS "Single Sender Row Re-Distribution"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -83)))) AS "Partition Level Locking"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -84)))) AS "Primary Index"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -85)))) AS "Secondary Index"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -86)))) AS "Composite Secondary Index"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -87)))) AS "Join Index"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -88)))) AS "Non Unique UnHashed Index"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -89)))) AS "Unique UnHashed Index"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -90)))) AS "Non Unique Hashed Index"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -91)))) AS "Unique Hashed Index"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -92)))) AS "Fastexport Protocol"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -93)))) AS "Fastload Protocol"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -94)))) AS "Multiload Protocol"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -95)))) AS "Archive/Restore"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -96)))) AS "MloadX Protocol"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -97)))) AS "Fast Path Function"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -98)))) AS "R Table Operator"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -99)))) AS "Bit Manipulation Function"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -100)))) AS "Analytics Extensions"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -101)))) AS "Partial Redistribution Partial Duplication"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -102)))) AS "Queryable View Column Info"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -103)))) AS "Multiple Count Distinct"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -104)))) AS "Aggregation Push for Union All"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -105)))) AS "Binary Join Push for Union All"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -106)))) AS "Script Table Operator"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -107)))) AS "Dataset Data Type"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -108)))) AS "Dot Notation"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -109)))) AS "Union All Pushdown"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -110)))) AS "In-Memory Enhancement for Outer Join"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -111)))) AS "In-Memory Enhancement for PRPD Join"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -112)))) AS "In-Memory All Rows One AMP Optimization"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -113)))) AS "In-Memory AVX2 Support"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -114)))) AS "InList Rewrite to Subquery"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -115)))) AS "InList Rewrite to Outer Join"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -116)))) AS "InList Rewrite disqualifed by MultiTable Expression"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -117)))) AS "InList Rewrite Threshold Exceeded"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -118)))) AS "Sparse Map"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -119)))) AS "Two Maps"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -120)))) AS "Not System-Default Map"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -121)))) AS "LEAD LAG Analytics"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -122)))) AS "Geospatial Index Single Table Predicate"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -123)))) AS "Geospatial Index Single Table Distance Predicate"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -124)))) AS "Geospatial Index Join Predicate"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -125)))) AS "Geospatial Index Distance Join Predicate"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -126)))) AS "Dataset Data Type Storage Format CSV"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -127)))) AS "Push TOP N into UNION ALL Derived Tables"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -128)))) AS "In-Memory Bulk Qualification Enhancements"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -129)))) AS "Time Series Table"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -130)))) AS "Soft RetLimit for number of rows AMP step returns"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -131)))) AS "Function Alias"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -132)))) AS "QueryGrid Remote Tables"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -133)))) AS "QueryGrid Common Remote Table Elimination"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -134)))) AS "Dynamic Parameterization of Literals Feature"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -135)))) AS "SET TRANSFORM"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -136)))) AS "ODBC Scalar Functions"
,ZEROIFNULL(SUM(GETBIT(A.FEATUREUSAGE,(2047 -137)))) AS "Multisource"
FROM PDCRINFO.DBQLOGTBL_HST A 
WHERE LogDate BETWEEN current_date - 30  AND current_date 
GROUP BY 1,2;


--------------------------------------------------------------


Query 14

SELECT					
	QryLog.LogDate	AS "Log Date"
	,Extract( Hour from starttime) AS "Log Hour"
	,username as UserName				
	,CASE WHEN StatementType = 'Merge Into' THEN 'Ingest & Prep'				
		  WHEN StatementType = 'Begin Loading' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'Mload' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'Collect Statistics' THEN 'Data Maintenance'			
		  WHEN StatementType = 'Delete' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'End Loading' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'Begin Delete Mload' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'Update' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'Select' THEN 'Answers'			
		  WHEN StatementType = 'Exec' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'Release Mload' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'Insert' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'Begin Mload' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'Execute Mload' THEN 'Ingest & Prep'			
		  WHEN StatementType = 'Commit Work' THEN 'Ingest & Prep'			
		  ELSE 'System/Procedural' END AS StatementOutcome			
	,StatementType				
	,clientid AS ClientId				
	,CASE				
                WHEN (QryLog.StatementType IN ('Insert', 'Update', 'Delete', 'Create Table', 'Merge Into')					
					OR QryLog.AppID LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%'))
                    THEN 'ETL/ELT'					
                WHEN QryLog.StatementType = 'Select'					
                    AND (AppID IN ('TPTEXP', 'FASTEXP') or appid like  'JDBCE%')   					
                    THEN 'EXPORT'					
                WHEN QryLog.StatementType = 'Select'					
                    AND QryLog.AppID NOT LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%', 'JDBCE%')					
                    THEN 'QUERY'					
				--WHEN QryLog.StatementType in ('Dump Database','Unrecognized type','Release Lock','Collect Statistics')	
				    --THEN 'ADMIN'	
                ELSE					
                    'OTHER'					
            END AS WorkLoadType					
	,'Complexity' AS Complexity				
            					
	--,UPPER(U.Department) as Department				
	--,UPPER(U.SubDepartment) as BusinessGroup				
	,SUM(QryLog.AMPCPUTime + QryLog.ParserCPUTime) (BIGINT) as SUMCPUTime				
	,SUM(QryLog.TotalIOCount) (BIGINT) as TotalIOCount				
	,COUNT(*) as QueryCount				
	FROM PDCRINFO.DBQLogTbl_hst QryLog				
			
	--	ON QryLog.UserName = U.UserName			
	WHERE QryLog.LogDate BETWEEN current_date -30 AND current_date -1				
	GROUP BY 1,2,3,4,5,6,7,8;	

	
---------------------------------------------------------------

Query 15

Select
    Rank()  OVER (Order by CURRENTPERM DESC ) as CURRENTPERMRnk
   ,LogDate
   ,DatabaseName
   ,CURRENTPERM/1E9 "DB Size GB"
   ,PEAKPERM/1E9 "PEAK DB Size GB"
   ,MAXPERM/1E9 "MAX DB Size GB"
   ,CURRENTPERMSKEW
   ,PERMPCTUSED
 FROM PDCRINFO.DatabaseSpace_Hst
 
 WHERE   Logdate = (select MAX(Logdate) from PDCRINFO.DatabaseSpace_Hst )

 
 ---------------------------------------------------------------------
 
 
 Query 16
 
 Select
    Rank()  OVER (Order by CURRENTPERM DESC ) as CURRENTPERMRnk
   ,c.year_of_calendar
   ,c.Month_of_Year
   ,c.Week_of_year
   ,LogDate
   ,Tablename
   ,DatabaseName
   ,AccountName
   ,CURRENTPERM/1E9 AS "Table Size GB"
   ,PEAKPERM/1E9 AS "PEAKPERM GB"
   ,CURRENTPERMSKEW
   ,PEAKPERMSKEW
 FROM PDCRINFO.TableSpace_Hst a INNER JOIN Sys_Calendar.CALENDAR  c  ON a.Logdate = c.Calendar_date  WHERE  c.Calendar_date = a.Logdate
  AND a.Logdate = (select MAX(Logdate) from PDCRINFO.TableSpace_Hst )
  Qualify CURRENTPERMRnk <= 30

  
  
  -----------------------------
  
  Query 17
  
  SELECT
	QryLog.LogDate AS "Log Date"
	,EXTRACT( HOUR FROM QryLog.starttime) AS "Log Hour"
	,username As UserName  
  --,(EXTRACT (MINUTE FROM QryLog.starttime)/10)*10 AS Log10Minute 	
	/*,CASE
       WHEN  c.Day_of_week  IN (1,7) THEN 'WE'
       WHEN  c.Day_of_week  IN (2,3,4,5,6) AND "Log Hour" =8  and Log10Minute IN (30,40,50) THEN 'BUS'
       WHEN  c.Day_of_week  IN (2,3,4,5,6) AND  "Log Hour"  between 9 and 18 THEN 'BUS'
       WHEN  c.Day_of_week  IN (2,3,4,5,6) AND  "Log Hour" IN (19,20,21,22,23,0,1,2,3)THEN 'BAT-OP'
       ELSE 'BAT-P'
       END OperatingWindow
	   */
	,CASE
       WHEN  c.Day_of_week  IN (1,7) THEN 'WE'
       WHEN  c.Day_of_week  IN (2,3,4,5,6) AND  "Log Hour"  between 8 and 18 THEN 'BUS'
       WHEN  c.Day_of_week  IN (2,3,4,5,6) AND  "Log Hour" IN (19,20,21,22,23,0,1,2,3)THEN 'BAT-OP'
       ELSE 'BAT-P'
       END OperatingWindow 
	
	--,UPPER(U.Department) as Department
	--,UPPER(U.SubDepartment) as BusinessGroup
	,SUM(QryLog.AMPCPUTime + QryLog.ParserCPUTime) (BIGINT) as SUMCPUTime
	,SUM(QryLog.TotalIOCount) (BIGINT) as TotalIOCount
	,COUNT(*) as QueryCount
	FROM PDCRINFO.DBQLogTbl QryLog
	INNER JOIN Sys_Calendar.CALENDAR  c  ON QryLog.LogDate = c.Calendar_date
	--INNER JOIN systemfe.ca_user_xref U
	--	ON QryLog.UserName = U.UserName
	WHERE QryLog.LogDate BETWEEN date -2 AND date -1
	and c.Calendar_date BETWEEN date -2 AND date -1
	GROUP BY 1,2,3,4;

	
	---------------------------------------------------------------------------------
  
  Query 18
  
  
  SELECT  					
		TMP.DatabaseName			
	,	TMP.Tablename			
	--,	TMP.LogDate			
	, TMP.currentperm				
	,SUM(CountOfUses) "Count Of Uses"				
	,SUM(TotalCPU1) "Total Cpu"				
	,SUM(IOInGBytes1)"Ioingbytes"				
					
	FROM(				
	SELECT 				
		o.QueryId QueryId			
	,	o.LogDate AS LogDate 			
	,	o.objectdatabasename     as "DatabaseName"			
	,	o.ObjectTableName        as "TableName"			
	,t.currentperm as currentperm 				
					
	,	(o.freqofuse)         as "CountOfUses"			
	,l.TotalCPU as TotalCPU				
	,l.IOInGBytes AS IOInGBytes				
	,	(t.currentperm*o.freqofuse/(sum(t.currentperm*o.freqofuse) over (partition by o.queryid))  )TableQueryPercent			
	,	(l.IOInGBytes*TableQueryPercent ) as "IOInGBytes1"			
	,	(l.TotalCpu*TableQueryPercent   ) as "TotalCPU1"			
					
					
					
					
					
	FROM PDCRINFO.DBQLObjTbl_hst o				
					
	LEFT JOIN 				
	(				
	Select				
   Tablename Tablename					
   ,DatabaseName DatabaseName					
   ,CURRENTPERM/1E9 AS currentperm					
    FROM PDCRINFO.TableSpace_Hst a INNER JOIN Sys_Calendar.CALENDAR  c  ON a.Logdate = c.Calendar_date  WHERE  c.Calendar_date = a.Logdate					
  AND a.Logdate = (select MAX(Logdate) from PDCRINFO.TableSpace_Hst )					
  Group By 1,2,3					
 					
	)t				
	on o.objectdatabasename = t.DatabaseName				
	AND o.ObjectTableName = t.Tablename				
	INNER JOIN 				
	(				
					
		SELECT 			
			l.LogDate AS LogDate 		
			,l.queryid queryid		
					
		,	COALESCE(spma.AvgIOPerReqGB, 1.0 / (1024 * 1024 * 1024))*l.TotalIOCount (FLOAT) AS IOInGBytes		
		,	l.AmpCPUTime + l.ParserCPUTime         AS TotalCPU		
					
		FROM PDCRINFO.DBQLogTbl_Hst  l			
		LEFT JOIN 			
		(			
			SELECT		
		a.thedate			
		,((SUM((a.LogicalDeviceReadKB + a.LogicalDeviceWriteKB) / (1024 * 1024) ) (float))			
					/ SUM(a.FileAcqs +a.FilePreReads +a.MemTextPageReads +a.MemCtxtPageWrites+a.MemCtxtPageReads+a.FileWrites ) )(decimal(18,10)) AvgIOPerReqGB
		FROM 			
		(			
		SELECT   			
		thedate thedate			
		,FileAcqs FileAcqs			
		,FilePreReads FilePreReads			
		,MemTextPageReads MemTextPageReads			
		,MemCtxtPageWrites MemCtxtPageWrites			
		,MemCtxtPageReads MemCtxtPageReads			
		,FileWrites FileWrites			
		,vproc1			
		,( FileAcqReadKB + FilePreReadKB +			
		  /* paging or swapping count times pagesize (= 4K) */			
		  (MemTextPageReads + MemCtxtPageReads ) * 4 ) AS LogicalDeviceReadKB			
		,( FileWriteKB +			
		  /* paging or swapping count times pagesize (= 4K) */			
		  MemCtxtPageWrites * 4 ) AS LogicalDeviceWriteKB			
		  FROM PDCRINFO.ResUsageSPMA_Hst			
		  WHERE  thedate BETWEEN date -1 and date -1			
		  GROUP BY 1,2,3,4,5,6,7,8,9,10			
		  )a			
		  Where   vproc1 > 0			
		  Group BY 1			
		) spma			
		ON l.LogDate = spma.thedate			
					
		Where l.LogDate BETWEEN date -1 and date -1			
					
					
		Group By 1,2,3,4			
	)l				
	ON o.queryid = l.queryid				
	AND o.logdate = l.logdate				
WHERE o.LogDate BETWEEN date -1 and date -1					
					
Group By 1,2,3,4,5,6,7,8					
)TMP					
Group By 1,2,3					


----------------------------------------------


--Query 19

Select * FROM DBC.Databases;

--Query 20

SELECT  DatabaseName,
        TableName,
        CreateTimeStamp,
        LastAlterTimeStamp
FROM    DBC.TablesV
WHERE   TableKind = 'T'
Group By 1,2,3,4;

----------------------------------------------

----------------------------------------------------------------------------------
	
	--Query 21
	
	-- Generate Node csv output for Gephi
-- Copy and paste to Nodes.csv, remove first column (ie: 1,2,3..n)
-- Make sure you leave in the header row, and that numbers do not have commas or quotes around them
-- save as csv format
--Query Node.csv
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
                    --FROM DBC.DBQLObjTbl /* uncomment for DBC */
					 FROM PDCRINFO.DBQLObjTbl_hst  /* uncomment for PDCR */
					WHERE Objecttype = 'Tab'
					AND ObjectTableName IS NOT NULL
                    AND ObjectColumnName IS NULL
					--AND CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for DBC */
					-- AND LogDate BETWEEN '2020-04-01' AND '2020-04-30' /* uncomment for PDCR */
					AND LogDate BETWEEN current_date - 90 AND current_date - 1 /* uncomment for PDCR */
					GROUP BY 1,2,3
                ) AS QTU1
                INNER JOIN
                (
                    SELECT   QueryID
                           , SUM(FreqOfUse) AS CountOfAllTableUses
                   -- FROM DBC.DBQLObjTbl /* uncomment for DBC */
					 FROM PDCRINFO.DBQLObjTbl_Hst  /* uncomment for PDCR */
					WHERE Objecttype = 'Tab'
					AND ObjectTableName IS NOT NULL
                    AND ObjectColumnName IS NULL
				--	AND CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for DBC */
				--	 AND LogDate BETWEEN '2020-04-01' AND '2020-04-30' /* uncomment for PDCR */
					AND LogDate BETWEEN current_date - 90 AND current_date - 1 /* uncomment for PDCR */
                    GROUP BY 1
                ) AS QTU2
                ON QTU1.QueryID=QTU2.QueryID
               
                --INNER JOIN DBC.DBQLogTbl QU /* uncomment for DBC */
                 INNER JOIN PDCRINFO.DBQLogTbl_Hst QU /* uncomment for PDCR */
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

    --FROM DBC.DBQLogTbl QU /* uncomment for DBC */
     FROM PDCRINFO.DBQLogTbl_Hst QU /* uncomment for PDCR */
		LEFT OUTER JOIN (
		SELECT    thedate
				, (sum((LogicalDeviceReadKB + LogicalDeviceWriteKB) / (1024 * 1024) ) (FLOAT))/ sum(FileAcqs +FilePreReads +MemTextPageReads +MemCtxtPageWrites+MemCtxtPageReads+FileWrites ) AS AvgIOPerReqGB
		FROM DBC.ResSpmaView
		WHERE  vproc1 > 0
		GROUP BY thedate
	    ) AS spma
	    ON QU.CollectTimeStamp (DATE) = spma.thedate
	--WHERE CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for DBC */
	-- WHERE LogDate BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for PDCR */
		AND LogDate BETWEEN current_date - 90 AND current_date - 1 /* uncomment for PDCR */

    ) AS SumAll
    ON 1=1
 
LEFT JOIN DBC.AllSpaceV SPACE
        ON SUM_ALLOC.DatabaseName=SPACE.DatabaseName
        AND SUM_ALLOC.TableName=SPACE.TableName
WHERE CPU_Sec > 0  -- FILTER: Only show affinity for resource consuming queries. Remove if you wish to see all.
--AND SUM_ALLOC.DatabaseName NOT IN ('database_name', 'another_database_name')  -- FILTER: change to remove databases like GCFR
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;

----------------------------------------------------------------------------------------------------------------------

--Query 22
-- Generate Edge csv output for Gephi
-- Copy and paste to Edges.csv, remove first column (ie: 1,2,3..n)
-- Make sure you leave in the header row, and that numbers do not have commas or quotes around them
-- save as csv format
--Query Node.csv

LOCKING ROW for ACCESS
SELECT    UPPER(TRIM(QTU1.DatabaseName) || '.' || TRIM(QTU1.TableName))  AS "Source"
        , UPPER(TRIM(QTU2.DatabaseName) || '.' || TRIM(QTU2.TableName))  AS "Target"
        , COUNT(DISTINCT QTU1.QueryID) AS "Weight"
		, 'Undirected' AS "Type"

FROM    (
                    SELECT   objectdatabasename AS DatabaseName
                           , ObjectTableName AS TableName
                           , QueryId
                    --FROM DBC.DBQLObjTbl /* uncomment for DBC */
					 FROM PDCRINFO.DBQLObjTbl_Hst  /* uncomment for PDCR */
					WHERE Objecttype = 'Tab'
					AND ObjectTableName IS NOT NULL
                    AND ObjectColumnName IS NULL
					--AND CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for DBC */
					-- AND LogDate BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for PDCR */
						AND LogDate BETWEEN current_date - 90 AND current_date - 1 /* uncomment for PDCR */
					GROUP BY 1,2,3
                ) AS QTU1
                INNER JOIN
                (
                    SELECT   objectdatabasename AS DatabaseName
                           , ObjectTableName AS TableName
                           , QueryId
                    --FROM DBC.DBQLObjTbl /* uncomment for DBC */
					 FROM PDCRINFO.DBQLObjTbl_Hst  /* uncomment for PDCR */
					WHERE Objecttype = 'Tab'
					AND ObjectTableName IS NOT NULL
                    AND ObjectColumnName IS NULL
					--AND CollectTimeStamp (DATE) BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for DBC */
					-- AND LogDate BETWEEN '2017-01-01' AND '2017-08-01' /* uncomment for PDCR */
						AND LogDate BETWEEN current_date - 90 AND current_date - 1 /* uncomment for PDCR */
					GROUP BY 1,2,3
                ) AS QTU2
                ON QTU1.QueryID=QTU2.QueryID
				--INNER JOIN DBC.DBQLogTbl QU /* uncomment for DBC */
                 INNER JOIN PDCRINFO.DBQLogTbl_Hst QU /* uncomment for PDCR */
                ON QTU1.QueryID=QU.QueryID
WHERE "Source" > "Target"  -- this ensures an edge only has one set, (a,b) never (a,b and b,a)
AND (QU.AMPCPUTime + QU.ParserCPUTime) > 0 -- FILTER: Only show affinity for resource consuming queries. Remove if you wish to see all.
--AND QTU1.DatabaseName NOT IN ('database_name', 'another_database_name')  -- FILTER: change to remove databases like GCFR

GROUP BY 1,2,4
;
---------------------------------------------------------------------------------------------------------------