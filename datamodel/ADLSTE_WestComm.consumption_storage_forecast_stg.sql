CREATE MULTISET GLOBAL TEMPORARY TABLE ADLSTE_WestComm.consumption_storage_forecast_stg ,FALLBACK ,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     LOG
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
PRIMARY INDEX ( SiteID ,"Report Date" ,"Log Date" )
ON COMMIT PRESERVE ROWS;
