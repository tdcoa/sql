CREATE SET TABLE ADLSTE_WestComm.Consumption_Storage_Forecast_V2 ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      SiteID VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      "Report Date" DATE FORMAT 'YYYY-MM-DD' NOT NULL,
      "Log Date" DATE FORMAT 'YYYY-MM-DD' NOT NULL,
      "Total Max Perm" BIGINT,
      "Total Current Perm" BIGINT,
      "Total Peak Perm" BIGINT,
      "Total Available Perm" BIGINT,
      "Total Current Pct" DECIMAL(18,4),
      "Total Available Pct" DECIMAL(18,4),
      "Moving Avg" DECIMAL(18,4),
      Trend DECIMAL(18,4),
      ReserveX DECIMAL(18,4),
      "Reserve Horizon" DECIMAL(18,4),
      SlopeX DECIMAL(18,4))
UNIQUE PRIMARY INDEX ( SiteID ,"Report Date" ,"Log Date" );
