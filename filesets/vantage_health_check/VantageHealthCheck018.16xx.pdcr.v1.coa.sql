/*
##############################################3
Query 18

Query Output File Name: TopTableBySize
Tableau Dashboard: Top Table Size

*/ 

/*{{save:TopTableBySize.csv}}*/
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
  AND a.Logdate = (select MAX(Logdate) from PDCRINFO.TableSpace_Hst );
