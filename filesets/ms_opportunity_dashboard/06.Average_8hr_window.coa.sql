
LOCK ROW FOR ACCESS 
SEL	CURRENT_DATE AS Date_Executed, (
SEL	 AVG((((s1.CPUUServ(DECIMAL(38, 6))) + s1.CPUUExec) / NULLIFZERO((s1.NCPUs(DECIMAL(38,
		6))))) / (s1.Secs(DECIMAL(38, 6)))) AS HourlyAvgCPUPct 
FROM	DBC.ResUsageSPMA s1 INNER JOIN sys_calendar.CALENDAR c1 
	ON  c1.calendar_date = s1.TheDate 
WHERE	TheDate BETWEEN(CURRENT_DATE - 30) 
	AND CURRENT_DATE )   AS avg_24hours, AVG(MovingAvg) AS avg_8hours 
FROM(
SELECT	'SiteId' AS SiteID, CURRENT_DATE(FORMAT'YYYY-MM-DD')(CHAR(10)) AS ReportDate,
		TheDate(FORMAT'YYYY-MM-DD')(CHAR(10)) AS LogDate, PeakStart || ':00:00' AS PeakStart,
		PeakEnd || ':00:00' AS PeakEnd , HourlyAvgCPUPct, AvgCPUPct(DECIMAL(18,
		4)) AS AvgCPUPct, 
CASE	
	WHEN Period_Number < 21 THEN NULL 
	WHEN AvgCPUPct IS NULL THEN NULL 
	ELSE MovingAvg END(DECIMAL(18, 4)) AS MovingAvg , Trend(DECIMAL(18,
		4)) AS Trend, ReserveX, 
CASE	
	WHEN Trend >= ReserveX THEN Trend 
	ELSE NULL END(DECIMAL(18, 4)) AS ReserveHorizon , SlopeX(DECIMAL(18,
		4)) AS SlopeX 
FROM(
SELECT	SiteID, COUNT(*) OVER(
ORDER BY calendar_date ROWS UNBOUNDED PRECEDING) AS Period_Number,
		Calendar_Date AS TheDate , HourlyAvgCPUPct, VPeakAvgCPUPct AS AvgCPUPct,
		AVG(VPeakAvgCPUPct) OVER(
ORDER BY  Calendar_Date ROWS 21 PRECEDING) AS MovingAvg , 
CASE	
	WHEN VPeakAvgCPUPct IS NOT NULL THEN 1 
	ELSE 0 
END	AS CountX, SUM(CountX) OVER() AS CountAll, ForecastX AS Trend,
		80 AS ReserveX, SlopeX, PeakStart, PeakEnd 
FROM(
SELECT	SiteID, Period_Number, a4.Month_Of_Calendar, a4.TheDate,
		HourlyAvgCPUPct, NULL(DECIMAL(38, 6)) AS VPeakAvgCPUPct, a4.TrendX,
		a4.SlopeX , NULL(CHAR(13)) AS PeakStart, NULL(CHAR(13)) AS PeakEnd,
		a4.TheHour, c2.calendar_date, COUNT(*) OVER(
ORDER BY c2.calendar_date ROWS UNBOUNDED PRECEDING) AS SequenceNbr ,
		a4.TrendX + (a4.SlopeX * SequenceNbr) AS ForecastX, VPeakAvgCPUPct AS ExtVPeakAvgCPUPct 
FROM(
SELECT	SiteID, Period_Number, Month_Of_Calendar, TheDate, HourlyAvgCPUPct,
		VPeakAvgCPUPct , CAST(REGR_INTERCEPT(VPeakAvgCPUPct, Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING) AS DECIMAL(30, 6)) + Period_Number * CAST((REGR_SLOPE(VPeakAvgCPUPct,
		Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING)) AS DECIMAL(30, 6)) AS TrendX , CAST(REGR_SLOPE(VPeakAvgCPUPct,
		Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING) AS DECIMAL(30, 6)) AS SlopeX , PeakStart,
		PeakEnd, TheHour 
FROM(
SELECT	SiteID, TheDate, TheHour, Month_Of_Calendar, PeakStart,
		PeakEnd, HourlyAvgCPUPct, VPeakAvgCPUPct, ROW_NUMBER() OVER(
ORDER BY TheDate) AS Period_Number 
FROM(
SELECT	SiteID, TheDate, TheHour, Month_Of_Calendar, PeakStart,
		PeakEnd, HourlyAvgCPUPct, VPeakAvgCPUPct 
FROM(
SELECT	'SiteID' AS SiteID, TheDate, c1.Month_Of_Calendar , (TheTime / 10000(SMALLINT)) AS TheHour,
		(TheDate(DATE, FORMAT 'YYYY-MM-DD')) || ' ' || TRIM(TheHour(FORMAT '99')) AS PeakEnd,
		AVG((((s1.CPUUServ(DECIMAL(38, 6))) + s1.CPUUExec) / NULLIFZERO((s1.NCPUs(DECIMAL(38,
		6))))) / (s1.Secs(DECIMAL(38, 6)))) AS HourlyAvgCPUPct  , AVG((HourlyAvgCPUPct(DECIMAL(38,
		6)))) OVER(
ORDER BY  TheDate, TheHour ROWS 7 PRECEDING) AS VPeakAvgCPUPct,
		MIN((TheDate(DATE, FORMAT 'YYYY-MM-DD')) || ' ' || TRIM(TheHour(FORMAT '99'))) OVER(
ORDER BY  TheDate, TheHour ROWS 7 PRECEDING) AS PeakStart 
FROM	DBC.ResUsageSPMA s1, sys_calendar.CALENDAR c1 
WHERE	 c1.calendar_date = s1.TheDate 
	AND c1.day_of_week IN(2, 3, 4, 5, 6) 
	AND s1.TheDate BETWEEN(CURRENT_DATE - 30) 
	AND CURRENT_DATE 
GROUP BY 1, 2, 3, 4) a1 
QUALIFY	ROW_NUMBER() OVER(PARTITION BY TheDate 
ORDER BY VPeakAvgCPUPct  DESC) = 1) a2) a3 
QUALIFY	ROW_NUMBER() OVER(
ORDER BY TheDate  DESC) = 1) a4, sys_calendar.CALENDAR c2 
WHERE	 c2.calendar_date BETWEEN a4.TheDate + 1 
	AND a4.TheDate + (365 * 2) 
	AND c2.day_of_week IN(2, 3, 4, 5, 6) 
UNION	
SELECT	SiteID, Period_Number, Month_Of_Calendar, TheDate, HourlyAvgCPUPct,
		VPeakAvgCPUPct, CAST(REGR_INTERCEPT(VPeakAvgCPUPct, Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING) AS DECIMAL(30, 6)) + Period_Number * CAST((REGR_SLOPE(VPeakAvgCPUPct,
		Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING)) AS DECIMAL(30, 6)) AS TrendX , CAST(REGR_SLOPE(VPeakAvgCPUPct,
		Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING) AS DECIMAL(30, 6)) AS SlopeX, PeakStart,
		PeakEnd, TheHour, TheDate, 0, TrendX, VPeakAvgCPUPct AS ExtVPeakAvgCPUPct 
FROM(
SELECT	SiteID, TheDate, TheHour, Month_Of_Calendar, PeakStart,
		PeakEnd, HourlyAvgCPUPct, VPeakAvgCPUPct, ROW_NUMBER() OVER(
ORDER BY TheDate) AS Period_Number 
FROM(
SELECT	SiteID, TheDate, TheHour, Month_Of_Calendar, PeakStart,
		PeakEnd, HourlyAvgCPUPct, VPeakAvgCPUPct 
FROM(
SELECT	'SiteID' AS SiteID, TheDate, c1.Month_Of_Calendar, (TheTime / 10000(SMALLINT)) AS TheHour,
		(TheDate(DATE, FORMAT 'YYYY-MM-DD')) || ' ' || TRIM(TheHour(FORMAT '99')) AS PeakEnd,
		AVG((((s1.CPUUServ(DECIMAL(38, 6))) + s1.CPUUExec) / NULLIFZERO((s1.NCPUs(DECIMAL(38,
		6))))) / (s1.Secs(DECIMAL(38, 6)))) AS HourlyAvgCPUPct , AVG((HourlyAvgCPUPct(DECIMAL(38,
		6)))) OVER(
ORDER BY  TheDate, TheHour ROWS 7 PRECEDING) AS VPeakAvgCPUPct,
		MIN((TheDate(DATE, FORMAT 'YYYY-MM-DD')) || ' ' || TRIM(TheHour(FORMAT '99'))) OVER(
ORDER BY  TheDate, TheHour ROWS 7 PRECEDING) AS PeakStart 
FROM	DBC.ResUsageSPMA s1, sys_calendar.CALENDAR c1 
WHERE	 c1.calendar_date = s1.TheDate 
	AND c1.day_of_week IN(2, 3, 4, 5, 6) 
	AND s1.TheDate BETWEEN(CURRENT_DATE - 30) 
	AND CURRENT_DATE 
GROUP BY 1, 2, 3, 4) a1 
QUALIFY	ROW_NUMBER() OVER(PARTITION BY TheDate 
ORDER BY VPeakAvgCPUPct  DESC) = 1) a2) a3) a4 
WHERE	ForecastX < 100) a5)a6;
