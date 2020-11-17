/*
##############################################3
Query 18

Query Output File Name: TopTableBySize
Tableau Dashboard: Top Table Size

*/ 

/*{{save:TopTableBySize.csv}}*/
 SELECT
     Rank()  OVER (Order by CURRENTPERM DESC ) as CURRENTPERMRnk
    ,EXTRACT(YEAR FROM Current_date) AS year_of_calendar
    ,EXTRACT(Month FROM Current_date)  AS Month_of_Year
    ,''  AS Week_of_year
    ,Case when extract(hour from current_time) LT 6 THEN current_date-1
          ELSE date END AS LogDate
    ,D.TVMNameI             AS Tablename
    ,D.DatabaseNameI        AS DatabaseName
    ,D.AccountName          AS AccountName
    ,dbsp.CurrentPerm/1E9   AS "Table Size GB"
    ,dbsp.PeakPerm /1E9     AS "PEAKPERM GB"
    ,dbsp.CurrentPermSkew   AS CURRENTPERMSKEW
    ,dbsp.PeakPermSkew      AS PEAKPERMSKEW
FROM
            (SELECT
                  dbsp.TableId                 AS TableId
                 ,dbsp.DatabaseId              AS DatabaseId
                 ,SUM(dbsp.CurrentPermSpace)   AS CurrentPerm
                 ,SUM(dbsp.PeakPermSpace)      AS PeakPerm
	         ,100 * (1-(AVG(dbsp.CURRENTPERMSPACE)/NULLIFZERO(MAX(dbsp.CURRENTPERMSPACE))))   AS CurrentPERMSKEW
	         ,100 * (1-(AVG(dbsp.PeakPERMSPACE)/NULLIFZERO(MAX(dbsp.PeakPERMSPACE))))         AS PeakPERMSKEW
	      FROM   DBC.DataBaseSpace  dbsp
	      WHERE dbsp.TableID <> '000000000000'XB
	        AND dbsp.CurrentPermSpace > 0
	      group by 1,2
	    ) dbsp,
	    (select dbase.databasenamei,
	            dbase.databaseid,
	            dbase.accountname,
	            tvm.tvmid,
	            tvm.tvmnamei
	      from DBC.Dbase dbase,
	           DBC.TVM tvm
	      where  TVM.DatabaseId  = Dbase.DatabaseId
	    ) d
  WHERE  dbsp.DatabaseId = D.DatabaseId
    AND dbsp.TableID = D.tvmid
;
