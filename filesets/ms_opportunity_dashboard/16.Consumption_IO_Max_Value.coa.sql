LOCK	ROW FOR ACCESS 
SEL	logdate, AVG(AvgIOPct) io_max_value 
FROM	(
SELECT	SiteID AS SiteID ,CURRENT_DATE (FORMAT'YYYY-MM-DD') (CHAR(10)) AS ReportDate,
		TheDate (FORMAT'YYYY-MM-DD') (CHAR(10)) AS LogDate , PeakStart || ':00:00' AS PeakStart,
		PeakEnd || ':00:00' AS PeakEnd, AvgIOPct(DECIMAL(18, 4)) AS AvgIOPct,
		MovingAvg(DECIMAL(18, 4)) AS MovingAvg  , Trend(DECIMAL(18, 4)) AS Trend,
		ReserveX, 
CASE	
	WHEN Trend >= ReserveX THEN Trend 
	ELSE NULL END(DECIMAL(18, 4)) AS ReserveHorizon , SlopeX(DECIMAL(18,
		4))AS SlopeX 
FROM(
SELECT	SiteID, COUNT(*) OVER(
ORDER BY calendar_date ROWS UNBOUNDED PRECEDING) AS Period_Number,
		Month_Of_Calendar, Calendar_Date AS TheDate, PeakStart, PeakEnd,
		VPeakAvgIOPct AS AvgIOPct, AVG(VPeakAvgIOPct) OVER(
ORDER BY  Calendar_Date ROWS 30 PRECEDING) AS MovingAvg , 
CASE	
	WHEN VPeakAvgIOPct IS NOT NULL THEN 1 
	ELSE 0 
END	AS CountX, SUM(CountX) OVER() AS CountAll, MIN(Trend * CountX) OVER() AS MINTrend,
		MAX(Trend * CountX) OVER() AS MAXTrend, ForecastX AS Trend, 80 AS ReserveX ,
		MIN(Month_Of_Calendar) OVER() AS MinMonthAll, MAX(Month_Of_Calendar) OVER() AS MAXMonthAll,
		(MAXMonthAll - MINMOnthAll) + 1 AS MonthsNumber , SlopeX 
FROM(
SELECT	a5.SiteID, a5.Period_Number, c2.Month_Of_Calendar, a5.TheDate,
		NULL(DECIMAL(38, 6)) AS VPeakAvgIOPct, a5.TrendX, a5.SlopeX,
		NULL(CHAR(13)) AS PeakStart, NULL(CHAR(13)) AS PeakEnd , a5.TheHour,
		c2.calendar_date, COUNT(*) OVER(
ORDER BY c2.calendar_date ROWS UNBOUNDED PRECEDING) AS SequenceNbr,
		a5.TrendX + (a5.SlopeX * SequenceNbr) AS ForecastX , VPeakAvgIOPct AS ExtVPeakAvgIOPct,
		COUNT(*) OVER() AS CountAll 
FROM	(
SELECT	SiteID, Period_Number, Month_Of_Calendar, TheDate, VPeakAvgIOPct,
		CAST(REGR_INTERCEPT(VPeakAvgIOPct, Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING) AS DECIMAL(30, 6)) + Period_Number * CAST((REGR_SLOPE(VPeakAvgIOPct,
		Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING)) AS DECIMAL(30, 6)) AS TrendX , CAST(REGR_SLOPE(VPeakAvgIOPct,
		Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING) AS DECIMAL(30, 6)) AS SlopeX , PeakStart,
		PeakEnd, TheHour 
FROM(
SELECT	SiteID, TheDate, TheHour, Month_Of_Calendar, PeakStart,
		PeakEnd, VPeakAvgIOPct , ROW_NUMBER() OVER(
ORDER BY TheDate) AS Period_Number 
FROM(
SELECT	SiteID, TheDate, TheHour, Month_Of_Calendar, PeakStart,
		PeakEnd, HourlyAvgIOPct, VPeakAvgIOPct 
FROM(
SELECT	SiteID, TheDate, Month_Of_Calendar, TheHour, (TheDate(DATE,
		FORMAT 'YYYY-MM-DD')) || ' ' || TRIM(TheHour(FORMAT '99')) AS PeakEnd ,
		AVG(MinDiskPct) AS HourlyAvgIOPct, AVG(HourlyAvgIOPct) OVER(
ORDER BY  SiteID, TheDate, TheHour ROWS 7 PRECEDING) AS VPeakAvgIOPct ,
		MIN((TheDate(DATE, FORMAT 'YYYY-MM-DD')) || ' ' || TRIM(TheHour(FORMAT '99'))) OVER(
ORDER BY  SiteID, TheDate, TheHour ROWS 7 PRECEDING) AS PeakStart 
FROM(
SELECT	'SiteID' AS SiteID, Month_Of_Calendar, TheDate, TheHour,
		TheMinute, MIN(DiskPct) AS MinDiskPct, AVG(DiskPct(DECIMAL(18,
		4))) AS AvgDiskPct , MAX(DiskPct) AS MaxDiskPct, MAX(TotalCount2) AS TotalActiveDevices,
		MIN(TotalCount3) - 1 AS CountDevicesBelow80th, COUNT(*) AS CountDevicesAbove80th,
		(CountDevicesBelow80th(DECIMAL(18, 4))) / TotalActiveDevices AS PctDevicesBelow80th,
		1 - PctDevicesBelow80th AS PctDevicesAbove80th 
FROM(
SELECT	TheDate, TheHour, Month_Of_Calendar, TheMinute, AvgDiskPct2,
		NodeID, CtlID, LdvID, DiskPct, COUNT(*) AS TotalCount, SUM(TotalCount) OVER(PARTITION BY TheDate,
		TheHour, TheMinute) AS TotalCount2 , (SUM(TotalCount) OVER(PARTITION BY TheDate,
		TheHour, TheMinute 
ORDER BY TheDate, TheHour, TheMinute, DiskPct, NodeID, CtlID, LdvID ROWS UNBOUNDED PRECEDING))  AS TotalCount3,
		(TotalCount3(DECIMAL(18, 4))) / (TotalCount2(DECIMAL(18, 4))) AS NumDiskPct 
FROM(
SELECT	s1.TheDate, c1.Month_Of_Calendar, EXTRACT(HOUR FROM s1.TheTime) AS TheHour,
		EXTRACT(MINUTE FROM s1.TheTime) AS TheMinute, s1.NodeID, s1.CtlID,
		s1.LdvID, (CAST(s1.ldvOutReqTime AS DECIMAL(18, 4)) / secs) AS DiskPct,
		AVG(DiskPct) OVER(PARTITION BY TheDate, TheHour, TheMinute) AS AvgDiskPct2 
FROM	PDCRINFO.ResUsageSldv_hst s1, sys_calendar.CALENDAR c1 
WHERE	 c1.calendar_date = s1.TheDate 
	AND s1.LdvType = 'DISK' 
	AND s1.ldvreads > 0 
	AND s1.TheDate BETWEEN(CURRENT_DATE - 91)
	AND CURRENT_DATE - 1) AS AA 
QUALIFY	NumDiskPct >= .80 
GROUP BY TheDate, Month_Of_Calendar, TheHour, TheMinute, AvgDiskPct2,
		NodeID, CtlID, LdvID, DiskPct) AS BB 
GROUP BY 1, 2, 3, 4, 5) AS CC 
GROUP BY 1, 2, 3, 4) a2 
QUALIFY	ROW_NUMBER() OVER(PARTITION BY TheDate 
ORDER BY VPeakAvgIOPct  DESC) = 1 ) a3) a4 
QUALIFY	ROW_NUMBER() OVER(
ORDER BY TheDate  DESC) = 1) a5, sys_calendar.CALENDAR c2 
WHERE	 c2.calendar_date BETWEEN a5.TheDate + 1 
	AND a5.TheDate + (365 * 2) 
UNION	
SELECT	SiteID, Period_Number, Month_Of_Calendar, TheDate, VPeakAvgIOPct,
		CAST(REGR_INTERCEPT(VPeakAvgIOPct, Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING) AS DECIMAL(30, 6)) + Period_Number * CAST((REGR_SLOPE(VPeakAvgIOPct,
		Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING)) AS DECIMAL(30, 6)) AS TrendX , CAST(REGR_SLOPE(VPeakAvgIOPct,
		Period_Number) OVER(ROWS BETWEEN UNBOUNDED PRECEDING 
	AND UNBOUNDED FOLLOWING) AS DECIMAL(30, 6)) AS SlopeX, PeakStart,
		PeakEnd, TheHour, TheDate,0 AS SequenceNbr, TrendX AS ForecastX,
		VPeakAvgIOPct AS ExtVPeakAvgIOPct, COUNT(*) OVER() AS CountAll 
FROM	(
SELECT	SiteID, TheDate, TheHour, Month_Of_Calendar, PeakStart,
		PeakEnd, VPeakAvgIOPct, ROW_NUMBER() OVER(
ORDER BY TheDate) AS Period_Number 
FROM	(
SELECT	SiteID, TheDate, TheHour, Month_Of_Calendar, PeakStart,
		PeakEnd, HourlyAvgIOPct, VPeakAvgIOPct 
FROM(
SELECT	SiteID , TheDate, Month_Of_Calendar, TheHour, (TheDate(DATE,
		FORMAT 'YYYY-MM-DD')) || ' ' || TRIM(TheHour(FORMAT '99')) AS PeakEnd,
		AVG(MinDiskPct) AS HourlyAvgIOPct, AVG(HourlyAvgIOPct) OVER(
ORDER BY  SiteID, TheDate, TheHour ROWS 7 PRECEDING) AS VPeakAvgIOPct,
		MIN((TheDate(DATE, FORMAT 'YYYY-MM-DD')) || ' ' || TRIM(TheHour(FORMAT '99'))) OVER(
ORDER BY  SiteID, TheDate, TheHour ROWS 7 PRECEDING) AS PeakStart 
FROM	(
SELECT	'SiteID' AS SiteID, Month_Of_Calendar, TheDate, TheHour,
		TheMinute, MIN(DiskPct) AS MinDiskPct, AVG(DiskPct(DECIMAL(18,
		4))) AS AvgDiskPct, MAX(DiskPct) AS MaxDiskPct, MAX(TotalCount2) AS TotalActiveDevices,
		MIN(TotalCount3) - 1 AS CountDevicesBelow80th, COUNT(*) AS CountDevicesAbove80th,
		(CountDevicesBelow80th(DECIMAL(18, 4))) / TotalActiveDevices AS PctDevicesBelow80th ,
		1 - PctDevicesBelow80th AS PctDevicesAbove80th 
FROM(
SELECT	TheDate, TheHour, Month_Of_Calendar, TheMinute, AvgDiskPct2,
		NodeID, CtlID, LdvID, DiskPct, COUNT(*) AS TotalCount, SUM(TotalCount) OVER(PARTITION BY TheDate,
		TheHour, TheMinute) AS TotalCount2, (SUM(TotalCount) OVER(PARTITION BY TheDate,
		TheHour, TheMinute 
ORDER BY TheDate, TheHour, TheMinute, DiskPct, NodeID, CtlID, LdvID ROWS UNBOUNDED PRECEDING))  AS TotalCount3 ,
		(TotalCount3(DECIMAL(18, 4))) / (TotalCount2(DECIMAL(18, 4))) AS NumDiskPct 
FROM	(
SELECT	s1.TheDate, c1.Month_Of_Calendar, EXTRACT(HOUR FROM s1.TheTime) AS TheHour,
		EXTRACT(MINUTE FROM s1.TheTime) AS TheMinute , s1.NodeID, s1.CtlID,
		s1.LdvID, (CAST(s1.ldvOutReqTime AS DECIMAL(18, 4)) / secs) AS DiskPct,
		AVG(DiskPct) OVER(PARTITION BY TheDate, TheHour, TheMinute) AS AvgDiskPct2 
FROM	PDCRINFO.ResUsageSldv_hst s1,  sys_calendar.CALENDAR c1 
WHERE	 c1.calendar_date = s1.TheDate 
	AND s1.LdvType = 'DISK' 
	AND s1.ldvreads > 0 
	AND s1.TheDate BETWEEN(CURRENT_DATE - 91) 
	AND CURRENT_DATE - 1 ) AS AA 
QUALIFY	NumDiskPct >= .80 
GROUP BY TheDate, Month_Of_Calendar, TheHour, TheMinute, AvgDiskPct2,
		NodeID, CtlID, LdvID, DiskPct) AS BB 
GROUP BY 1, 2, 3, 4, 5) AS CC 
GROUP BY 1, 2, 3, 4) a2 
QUALIFY	ROW_NUMBER() OVER(PARTITION BY TheDate 
ORDER BY VPeakAvgIOPct  DESC) = 1) a3) a4) a6 
WHERE	ForecastX < 100) a7)a8 
GROUP BY 1 
HAVING	io_max_value IS NOT NULL;
