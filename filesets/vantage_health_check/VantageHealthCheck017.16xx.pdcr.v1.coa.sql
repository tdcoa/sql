/*
##############################################3
Query 17

Query Output File Name: TopDatabasebySpace
Tableau Dashboard: Database Size, Top Database Size

*/

/*{{save:TopDatabasebySpace.csv}}*/

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

 WHERE   Logdate = (select MAX(Logdate) from PDCRINFO.DatabaseSpace_Hst );
