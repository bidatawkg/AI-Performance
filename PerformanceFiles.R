#!/usr/bin/env Rscript

#' @author Carmelo Rodriguez Martinez \email{carmelor@@yyy-net.com}
#' @keywords CashBack
#' 
#' 
source("C:/Users/Administrator/Documents/scripts/setup_BI_DATA.R")

# log file
log_file <- "C:/Users/Administrator/Documents/scripts/AI-Performance/logs/PerformanceFiles.R.log"

script_name <- "PerformanceFiles.R"
write_log("****************************************************************************************")
write_log(paste0("Starting script: ",basename(script_name)))

# Establish the connection
execution_time <- Sys.time()
write_log(paste0("Starting at ",execution_time))
write_log("****************************************************************************************")

# Define retry parameters
max_retries <- 4
retry_interval <- 5 * 60  # 5 minutes in seconds

retry_attempts <- 0
connection_successful <- FALSE

while (retry_attempts < max_retries && !connection_successful) {
  tryCatch({
    
    conn <- dbConnect(odbc::odbc(), .connection_string = conn_str)
    connection_successful <- TRUE  # Connection successful
    write_log("Successful connection.")
    
    ## Delete all past files
    
    # List all files in the folder (non-recursive, just in aiPerformanceFiles/)
    all_files <- list.files("aiPerformanceFiles", full.names = TRUE)
    
    # Extract just the file names (without path)
    file_names <- basename(all_files)
    
    # Try to parse the starting part of each file name as a date (YYYY-MM-DD)
    file_dates <- as.Date(substr(file_names, 1, 10), format = "%Y-%m-%d")
    
    # Identify files with valid dates that are strictly before today
    past_files <- all_files[!is.na(file_dates) & file_dates < Sys.Date()]
    
    # Delete silently (suppress TRUE/FALSE output)
    if (length(past_files) > 0) {
      invisible(file.remove(past_files))
    }
    write_log("Past files deleted!")
    
    # Define yesterday
    yesterday <- Sys.Date() - 1
    
    # First day of the month (based on yesterday)
    mtd_start <- as.Date(format(yesterday, "%Y-%m-01"))
    
    #####################################################################
    ############################ Main Targets ###########################
    #####################################################################
    
    query <- "
    -- 1) Date window: first day of the month three months ago through today
    DECLARE @StartDate DATE = DATEADD(DAY, -60, CAST(GETDATE() AS DATE));
    DECLARE @EndDate   date = CAST(GETDATE() AS date);
    
    SELECT
        b.[Date],

        -- 3) Sum all metrics
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    WHERE
        b.[Date] >= @StartDate
        AND b.[Date] <= @EndDate
    GROUP BY
        b.[Date]
    "
    
    df <- dbGetQuery(conn, query)
    
    df_main <- bind_rows(
      # Daily
      df %>%
        filter(Date == yesterday) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "Yesterday"),
      
      # Weekly (yesterday + 6 days back)
      df %>%
        filter(Date >= yesterday - 6 & Date <= yesterday) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "7 days"),
      
      # Monthly (yesterday + 29 days back)
      df %>%
        filter(Date >= yesterday - 29 & Date <= yesterday) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "30 days"),
      
      # MTD (1st of current month through yesterday)
      df %>%
        filter(Date >= mtd_start & Date <= yesterday) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "MTD")
    )
    # Add margin
    df_main$Margin <- round((df_main$GGR / df_main$STAKE) * 100, digits = 1)
    
    # Add market
    df_main$Market <-"ALL"
    
    # Same by markets
    query <- "
    -- 1) Date window: first day of the month three months ago through today
    DECLARE @StartDate DATE = DATEADD(DAY, -60, CAST(GETDATE() AS DATE));
    DECLARE @EndDate   date = CAST(GETDATE() AS date);
    
    SELECT
        b.[Date],
        us.COUNTRY AS Market,
        
        -- 3) Sum all metrics
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= @StartDate
        AND b.[Date] <= @EndDate
        AND us.COUNTRY IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
    GROUP BY
        b.[Date], us.COUNTRY
    "
    
    df_markets <- dbGetQuery(conn, query)
    df_main_markets <- bind_rows(
      # Daily
      df_markets %>%
        filter(Date == yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "Yesterday"),
      
      # Weekly (yesterday + 6 days back)
      df_markets %>%
        filter(Date >= yesterday - 6 & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "7 days"),
      
      # Monthly (yesterday + 29 days back)
      df_markets %>%
        filter(Date >= yesterday - 29 & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "30 days"),
      
      # MTD (1st of current month through yesterday)
      df_markets %>%
        filter(Date >= mtd_start & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "MTD")
    )
    
    # Add margin
    df_main_markets$Margin <- round((df_main_markets$GGR / df_main_markets$STAKE) * 100, digits = 1)
    
    # Other markets
    query <- "
    -- 1) Date window: first day of the month three months ago through today
    DECLARE @StartDate DATE = DATEADD(DAY, -60, CAST(GETDATE() AS DATE));
    DECLARE @EndDate   date = CAST(GETDATE() AS date);
    
    SELECT
        b.[Date],

        -- 3) Sum all metrics
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= @StartDate
        AND b.[Date] <= @EndDate
        AND us.COUNTRY NOT IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
    GROUP BY
        b.[Date]
    "
    
    df_others <- dbGetQuery(conn, query)
    
    df_main_others <- bind_rows(
      # Daily
      df_others %>%
        filter(Date == yesterday) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "Yesterday"),
      
      # Weekly (yesterday + 6 days back)
      df_others %>%
        filter(Date >= yesterday - 6 & Date <= yesterday) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "7 days"),
      
      # Monthly (yesterday + 29 days back)
      df_others %>%
        filter(Date >= yesterday - 29 & Date <= yesterday) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "30 days"),
      
      # MTD (1st of current month through yesterday)
      df_others %>%
        filter(Date >= mtd_start & Date <= yesterday) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "MTD")
    )
    
    # Add margin
    df_main_others$Margin <- round((df_main_others$GGR / df_main_others$STAKE) * 100, digits = 1)
    
    # Add market
    df_main_others$Market <-"Others"

    # Brands
    query <- "
    -- 1) Date window: first day of the month three months ago through today
    DECLARE @StartDate DATE = DATEADD(DAY, -60, CAST(GETDATE() AS DATE));
    DECLARE @EndDate   date = CAST(GETDATE() AS date);
    
    SELECT
        b.[Date],
        m.Market,
        SUM(b.DEPOSIT_AMOUNT) AS DEPs,
        SUM(b.Total_STAKE)    AS STAKE,
        SUM(b.Total_GGR)      AS GGR,
        SUM(b.Total_NGR)      AS NGR
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    JOIN bi_data.dbo.Users AS us
      ON us.GL_ACCOUNT = b.PARTYID
    CROSS APPLY (
        VALUES (
            CASE
                WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                WHEN us.BRANDID = 6                           THEN 'BET'
                ELSE NULL
            END
        )
    ) AS m(Market)
    WHERE
        b.[Date] >= @StartDate
        AND b.[Date] <= @EndDate
        AND m.Market IS NOT NULL         -- drop rows not in GCC or BET
    GROUP BY
        b.[Date],
        m.Market
    "
    
    df_brands <- dbGetQuery(conn, query)
    
    df_main_brands <- bind_rows(
      # Daily
      df_brands %>%
        filter(Date == yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "Yesterday"),
      
      # Weekly (yesterday + 6 days back)
      df_brands %>%
        filter(Date >= yesterday - 6 & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "7 days"),
      
      # Monthly (yesterday + 29 days back)
      df_brands %>%
        filter(Date >= yesterday - 29 & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(
          DEPs  = sum(DEPs, na.rm = TRUE),
          STAKE = sum(STAKE, na.rm = TRUE),
          GGR   = sum(GGR, na.rm = TRUE),
          NGR   = sum(GGR, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(Period = "30 days"),
      
      # MTD (1st of current month through yesterday)
      df_brands %>%
        filter(Date >= mtd_start & Date <= yesterday) %>%
        group_by(Market) %>%
        summarise(DEPs = sum(DEPs, na.rm = TRUE),
                  STAKE = sum(STAKE, na.rm = TRUE),
                  GGR = sum(GGR, na.rm = TRUE),
                  NGR = sum(NGR, na.rm = TRUE)) %>%
        mutate(Period = "MTD")
    )
    
    # Add margin
    df_main_brands$Margin <- round((df_main_brands$GGR / df_main_brands$STAKE) * 100, digits = 1)
    
    # Retention
    query <- "
      DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
      
      -- MTD window (1st of current month .. yesterday)
      DECLARE @MTD_Start        DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
      DECLARE @MTD_End          DATE = @Yesterday;
      
      -- Previous month window for MTD (1st of prev month .. same number of days as current MTD, capped at prev EOM)
      DECLARE @PrevMonthStart   DATE = DATEADD(MONTH, -1, @MTD_Start);
      DECLARE @PrevMTD_End      DATE = CASE
                                         WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                              THEN EOMONTH(@PrevMonthStart)
                                         ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                       END;
      
      WITH Periods AS (
          SELECT 'Yesterday' AS Period,
                 @Yesterday  AS StartDate, @Yesterday AS EndDate,
                 DATEADD(DAY, -1, @Yesterday) AS PrevStartDate, DATEADD(DAY, -1, @Yesterday) AS PrevEndDate
          UNION ALL
          SELECT '7 days',
                 DATEADD(DAY, -6, @Yesterday), @Yesterday,
                 DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday)
          UNION ALL
          SELECT '30 days',
                 DATEADD(DAY, -29, @Yesterday), @Yesterday,
                 DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday)
          UNION ALL
          SELECT 'MTD',
                 @MTD_Start, @MTD_End,
                 @PrevMonthStart, @PrevMTD_End
      )
      SELECT
          p.Period,
          Curr.RMP AS RMPs,
          Curr.FTD AS FTDs,
          ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
      FROM Periods AS p
      CROSS APPLY (
          SELECT
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
              COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
      ) AS Curr
      CROSS APPLY (
          SELECT
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
      ) AS Prev
        "
    
    df_ret <- dbGetQuery(conn, query)
    
    # Add Market
    df_ret$Market <-"ALL"
    
    # By market
    
    query <- "
      DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
      
      -- MTD window (1st of current month .. yesterday)
      DECLARE @MTD_Start      DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
      DECLARE @MTD_End        DATE = @Yesterday;
      
      -- Previous month window for MTD (same number of days, capped to prior EOM)
      DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
      DECLARE @PrevMTD_End    DATE = CASE
                                       WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                            THEN EOMONTH(@PrevMonthStart)
                                       ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                     END;
      
      WITH Periods AS (
          SELECT 'Yesterday' AS Period,
                 @Yesterday  AS StartDate, @Yesterday AS EndDate,
                 DATEADD(DAY, -1, @Yesterday) AS PrevStartDate, DATEADD(DAY, -1, @Yesterday) AS PrevEndDate
          UNION ALL
          SELECT '7 days',
                 DATEADD(DAY, -6, @Yesterday), @Yesterday,
                 DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday)
          UNION ALL
          SELECT '30 days',
                 DATEADD(DAY, -29, @Yesterday), @Yesterday,
                 DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday)
          UNION ALL
          SELECT 'MTD',
                 @MTD_Start, @MTD_End,
                 @PrevMonthStart, @PrevMTD_End
      )
      SELECT
          Curr.Market AS Market,
          p.Period,
          Curr.RMP AS RMPs,
          Curr.FTD AS FTDs,
          ROUND((1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
      FROM Periods AS p
      CROSS APPLY (
          SELECT
              us.COUNTRY AS Market,
              SUM(CASE WHEN b.RMP = 1 THEN 1 ELSE 0 END) AS RMP,
              SUM(CASE WHEN b.FTD = 1 THEN 1 ELSE 0 END) AS FTD
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
            AND us.COUNTRY IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
          GROUP BY us.COUNTRY
      ) AS Curr
      OUTER APPLY (
          SELECT
              SUM(CASE WHEN b.RMP = 1 THEN 1 ELSE 0 END) AS RMP
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
            AND us.COUNTRY = Curr.Market
      ) AS Prev
        "
    
    df_ret_markets <- dbGetQuery(conn, query)

    # Others
    
    query <- "
      DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
      
      -- MTD window (1st of current month .. yesterday)
      DECLARE @MTD_Start        DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
      DECLARE @MTD_End          DATE = @Yesterday;
      
      -- Previous month window for MTD (1st of prev month .. same number of days as current MTD, capped at prev EOM)
      DECLARE @PrevMonthStart   DATE = DATEADD(MONTH, -1, @MTD_Start);
      DECLARE @PrevMTD_End      DATE = CASE
                                         WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                              THEN EOMONTH(@PrevMonthStart)
                                         ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                       END;
      
      WITH Periods AS (
          SELECT 'Yesterday' AS Period,
                 @Yesterday  AS StartDate, @Yesterday AS EndDate,
                 DATEADD(DAY, -1, @Yesterday) AS PrevStartDate, DATEADD(DAY, -1, @Yesterday) AS PrevEndDate
          UNION ALL
          SELECT '7 days',
                 DATEADD(DAY, -6, @Yesterday), @Yesterday,
                 DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday)
          UNION ALL
          SELECT '30 days',
                 DATEADD(DAY, -29, @Yesterday), @Yesterday,
                 DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday)
          UNION ALL
          SELECT 'MTD',
                 @MTD_Start, @MTD_End,
                 @PrevMonthStart, @PrevMTD_End
      )
      SELECT
          p.Period,
          Curr.RMP AS RMPs,
          Curr.FTD AS FTDs,
          ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
      FROM Periods AS p
      CROSS APPLY (
          SELECT
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
              COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
            AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
      ) AS Curr
      CROSS APPLY (
          SELECT
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
            AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
      ) AS Prev;
        "
    
    df_ret_others <- dbGetQuery(conn, query)
    df_ret_others$Market <-"Others"
    
    # Brands
    query <- "
      DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
      
      -- MTD window (1st of current month .. yesterday)
      DECLARE @MTD_Start        DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
      DECLARE @MTD_End          DATE = @Yesterday;
      
      -- Previous month window for MTD (1st of prev month .. same number of days as current MTD, capped at prev EOM)
      DECLARE @PrevMonthStart   DATE = DATEADD(MONTH, -1, @MTD_Start);
      DECLARE @PrevMTD_End      DATE = CASE
                                         WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                              THEN EOMONTH(@PrevMonthStart)
                                         ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                       END;
      
      WITH Periods AS (
          SELECT 'Yesterday' AS Period,
                 @Yesterday  AS StartDate, @Yesterday AS EndDate,
                 DATEADD(DAY, -1, @Yesterday) AS PrevStartDate, DATEADD(DAY, -1, @Yesterday) AS PrevEndDate
          UNION ALL
          SELECT '7 days',
                 DATEADD(DAY, -6, @Yesterday), @Yesterday,
                 DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday)
          UNION ALL
          SELECT '30 days',
                 DATEADD(DAY, -29, @Yesterday), @Yesterday,
                 DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday)
          UNION ALL
          SELECT 'MTD',
                 @MTD_Start, @MTD_End,
                 @PrevMonthStart, @PrevMTD_End
      )
      SELECT
          p.Period,
          Curr.Market,
          Curr.RMP AS RMPs,
          Curr.FTD AS FTDs,
          ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
      FROM Periods AS p
      CROSS APPLY (
          SELECT
              m.Market,
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
              COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          CROSS APPLY (
              VALUES (
                  CASE
                      WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                      WHEN us.BRANDID = 6                           THEN 'BET'
                      ELSE NULL
                  END
              )
          ) AS m(Market)
          WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
            AND m.Market IS NOT NULL
          GROUP BY m.Market
      ) AS Curr
      OUTER APPLY (
          SELECT
              COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
          FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
          JOIN bi_data.dbo.Users AS us
            ON us.GL_ACCOUNT = b.PARTYID
          CROSS APPLY (
              VALUES (
                  CASE
                      WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                      WHEN us.BRANDID = 6                           THEN 'BET'
                      ELSE NULL
                  END
              )
          ) AS m(Market)
          WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
            AND m.Market = Curr.Market
      ) AS Prev;
    "
    
    df_ret_brands <- dbGetQuery(conn, query)
    
        
    # Add all together
    df_main <- rbind(df_main, df_main_markets, df_main_others, df_main_brands)
    df_retention <- rbind(df_ret, df_ret_markets, df_ret_others, df_ret_brands)
    
    df_main <- df_main %>%
      pivot_longer(
        cols = c(DEPs, STAKE, GGR, NGR, Margin),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Period, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")

    df_retention <- df_retention %>%
      pivot_longer(
        cols = c(RMPs, FTDs, Retention),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Period, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    df_main_final <- rbind(df_main, df_retention)
    
    # Total Monthly global values (excluding ALL)
    monthly_shares <- df_main_final %>%
      filter(Period == "30 days", METRIC %in% c("DEPs", "NGR"), Market != "ALL") %>%
      group_by(METRIC) %>%
      mutate(
        global_monthly_total = sum(VALUE, na.rm = TRUE),
        share = VALUE / global_monthly_total
      ) %>%
      ungroup() %>%
      select(Market, METRIC, share)

    
    # days 1..day(yesterday) of the month
    X <- as.integer(yesterday - mtd_start) + 1L
    
    df_main_final <- df_main_final %>%
      left_join(monthly_shares, by = c("Market","METRIC")) %>%
      mutate(
        Target = case_when(
          # --- DEPs: ALL
          METRIC == "DEPs" & Market == "ALL" & Period == "30 days"   ~ 4000000,
          METRIC == "DEPs" & Market == "ALL" & Period == "7 days"    ~ 4000000 * 7/30,
          METRIC == "DEPs" & Market == "ALL" & Period == "Yesterday" ~ 4000000 / 30,
          METRIC == "DEPs" & Market == "ALL" & Period == "MTD"       ~ 4000000 * X/30,
          
          # --- DEPs: other markets
          METRIC == "DEPs" & Market != "ALL" & Period == "30 days"   ~ share * 4000000,
          METRIC == "DEPs" & Market != "ALL" & Period == "7 days"    ~ share * 4000000 * 7/30,
          METRIC == "DEPs" & Market != "ALL" & Period == "Yesterday" ~ share * 4000000 / 30,
          METRIC == "DEPs" & Market != "ALL" & Period == "MTD"       ~ share * 4000000 * X/30,
          
          # --- NGR: ALL
          METRIC == "NGR" & Market == "ALL" & Period == "30 days"    ~ 2600000,
          METRIC == "NGR" & Market == "ALL" & Period == "7 days"     ~ 2600000 * 7/30,
          METRIC == "NGR" & Market == "ALL" & Period == "Yesterday"  ~ 2600000 / 30,
          METRIC == "NGR" & Market == "ALL" & Period == "MTD"        ~ 2600000 * X/30,
          
          # --- NGR: other markets
          METRIC == "NGR" & Market != "ALL" & Period == "30 days"    ~ share * 2600000,
          METRIC == "NGR" & Market != "ALL" & Period == "7 days"     ~ share * 2600000 * 7/30,
          METRIC == "NGR" & Market != "ALL" & Period == "Yesterday"  ~ share * 2600000 / 30,
          METRIC == "NGR" & Market != "ALL" & Period == "MTD"        ~ share * 2600000 * X/30,
          
          # --- Retention ---
          METRIC == "Retention" ~ 80,
          
          # --- Margin ---
          METRIC == "Margin" ~ 5,
          
          # --- Other metrics ---
          TRUE ~ 0
        ),
        DIFF_ABSOLUTE   = VALUE - Target,
        DIFF_PERCENTAGE = ifelse(Target == 0, NA, round((VALUE - Target) / Target * 100, 2))
      )
    df_main_final$share = NULL

    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0(getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(), "_ai-summary-targets_", mkt, ".csv"),
        row.names = FALSE
      )
    }
    
    write_log("Summary targets files created.")
    
    #####################################################################
    ######################### Main metrics Graph ########################
    #####################################################################    
    
    query <- "

    SELECT
        b.[Date],

        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    WHERE
        b.[Date] >= '2024-11-01'
        
    GROUP BY
        b.[Date]
    "
    df <- dbGetQuery(conn, query) 
    
    # Add Market and margin
    df$Margin <- round((df$GGR / df$STAKE) * 100, digits = 1)
    df$Market <-"ALL"
    
    
    query <- "

    SELECT
        b.[Date],
        us.COUNTRY AS Market,
    
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b

    INNER JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= '2024-11-01' 
        AND us.COUNTRY IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
        
    GROUP BY
        b.[Date],
        us.COUNTRY
    "
    df_markets <- dbGetQuery(conn, query)
    
    # Add margin
    df_markets$Margin <- round((df_markets$GGR / df_markets$STAKE) * 100, digits = 1)
    
    query <- "

    SELECT
        b.[Date],
        SUM(b.DEPOSIT_AMOUNT) AS DEPs,
        SUM(b.Total_STAKE)    AS STAKE,
        SUM(b.Total_GGR)      AS GGR,
        SUM(b.Total_NGR)      AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    JOIN bi_data.dbo.Users AS us
      ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= '2024-11-01'
        AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
    GROUP BY
        b.[Date];
    "
    df_others <- dbGetQuery(conn, query) 
    
    df_others$Margin <- round((df_others$GGR / df_others$STAKE) * 100, digits = 1)
    df_others$Market <-"Others"
    
    query <- "

    SELECT
        b.[Date],
        m.Market,
        SUM(b.DEPOSIT_AMOUNT) AS DEPs,
        SUM(b.Total_STAKE)    AS STAKE,
        SUM(b.Total_GGR)      AS GGR,
        SUM(b.Total_NGR)      AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    JOIN bi_data.dbo.Users AS us
      ON us.GL_ACCOUNT = b.PARTYID
    CROSS APPLY (
        VALUES (
            CASE
                WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                WHEN us.BRANDID = 6                           THEN 'BET'
                ELSE NULL
            END
        )
    ) AS m(Market)
    WHERE
        b.[Date] >= '2024-11-01'
        AND m.Market IS NOT NULL
    GROUP BY
        b.[Date],
        m.Market;
    "
    df_brands <- dbGetQuery(conn, query)
    
    # Add margin
    df_brands$Margin <- round((df_brands$GGR / df_brands$STAKE) * 100, digits = 1) 
    
    # Add all together
    df <- rbind(df, df_markets, df_others, df_brands)

    df <- df %>%
      pivot_longer(
        cols = c(DEPs, STAKE, GGR, NGR, Margin, RMPs, FTDs),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    # Round all except margin
    df <- df %>%
      mutate(VALUE = if_else(METRIC != "Margin", round(VALUE, 0), VALUE))
    
    # Retention
    
    # --- params ---
    cutoff_date <- as.Date("2024-11-01")  # lower bound for Date/Bucket
    
    # --- SQL builder (sargable, half-open ranges, no CAST on [Date] in WHERE) ---
    build_sql <- function(period = c("weekly","monthly","mtd"),
                          slice  = c("all","country","others","brand"),
                          cutoff = cutoff_date) {
      
      period <- match.arg(tolower(period), c("weekly","monthly","mtd"))
      slice  <- match.arg(tolower(slice),  c("all","country","others","brand"))
      cutoff <- format(as.Date(cutoff), "%Y-%m-%d")
      
      weekly_header <- "
SET NOCOUNT ON;
SET DATEFIRST 1;
DECLARE @Yesterday   date      = DATEADD(DAY, -1, CAST(GETDATE() AS date));
DECLARE @Tomorrow    date      = DATEADD(DAY,  1, @Yesterday);
DECLARE @CutoffDate  date      = '{cutoff}';
DECLARE @CutoffStart datetime2 = CAST(@CutoffDate AS datetime2(0));
DECLARE @EndBound    datetime2 = CAST(@Tomorrow   AS datetime2(0)); -- half-open [start, end)
"
      monthly_header <- "
SET NOCOUNT ON;
DECLARE @Yesterday   date      = DATEADD(DAY, -1, CAST(GETDATE() AS date));
DECLARE @Tomorrow    date      = DATEADD(DAY,  1, @Yesterday);
DECLARE @CutoffDate  date      = '{cutoff}';
DECLARE @CutoffStart datetime2 = CAST(@CutoffDate AS datetime2(0));
DECLARE @EndBound    datetime2 = CAST(@Tomorrow   AS datetime2(0)); -- half-open
"
      mtd_header <- "
SET NOCOUNT ON;
DECLARE @Yesterday   date      = DATEADD(DAY, -1, CAST(GETDATE() AS date));
DECLARE @Tomorrow    date      = DATEADD(DAY,  1, @Yesterday);
DECLARE @CutoffDate  date      = '{cutoff}';
DECLARE @CutoffStart datetime2 = CAST(@CutoffDate AS datetime2(0));
DECLARE @EndBound    datetime2 = CAST(@Tomorrow   AS datetime2(0)); -- half-open
DECLARE @CutoffDay   int       = DAY(@Yesterday);  -- cap day-of-month for MTD buckets
"
      
      # ---------- WEEKLY (Monday-start) ----------
      q_weekly_all <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
),
Dated AS (
  SELECT RawDate, DATEADD(DAY, 1 - DATEPART(WEEKDAY, RawDate), RawDate) AS DateBucket
  FROM Base
),
Agg AS (
  SELECT DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b
  JOIN Dated d ON d.RawDate=b.RawDate
  WHERE d.DateBucket >= @CutoffDate
  GROUP BY DateBucket
)
SELECT
  Market='ALL', Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Date;"
  
  q_weekly_country <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD, us.COUNTRY
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT=b.PARTYID
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
    AND us.COUNTRY IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
),
Dated AS (
  SELECT RawDate, COUNTRY AS Market,
         DATEADD(DAY, 1 - DATEPART(WEEKDAY, RawDate), RawDate) AS DateBucket
  FROM Base
),
Agg AS (
  SELECT d.Market AS Market, DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b
  JOIN Dated d ON d.RawDate=b.RawDate AND d.Market=b.COUNTRY
  WHERE d.DateBucket >= @CutoffDate
  GROUP BY d.Market, DateBucket
)
SELECT
  Market, Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Market, Date;"

q_weekly_others <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD, us.COUNTRY
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT=b.PARTYID
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
    AND (us.COUNTRY IS NULL OR us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ'))
),
Dated AS (
  SELECT RawDate, DATEADD(DAY, 1 - DATEPART(WEEKDAY, RawDate), RawDate) AS DateBucket
  FROM Base
),
Agg AS (
  SELECT DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b
  JOIN Dated d ON d.RawDate=b.RawDate
  WHERE d.DateBucket >= @CutoffDate
  GROUP BY DateBucket
)
SELECT
  Market='Others', Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Date;"

q_weekly_brand <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD,
         CASE WHEN us.BRANDID=1 AND (us.COUNTRY IS NULL OR us.COUNTRY<>'EG') THEN 'GCC'
              WHEN us.BRANDID=6 THEN 'BET' END AS Market
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT=b.PARTYID
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
    AND ((us.BRANDID=1 AND (us.COUNTRY IS NULL OR us.COUNTRY<>'EG')) OR us.BRANDID=6)
),
Dated AS (
  SELECT RawDate, Market,
         DATEADD(DAY, 1 - DATEPART(WEEKDAY, RawDate), RawDate) AS DateBucket
  FROM Base
),
Agg AS (
  SELECT d.Market AS Market, DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b
  JOIN Dated d ON d.RawDate=b.RawDate AND d.Market=b.Market
  WHERE d.DateBucket >= @CutoffDate
  GROUP BY d.Market, DateBucket
)
SELECT
  Market, Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Market, Date;"

# ---------- MONTHLY ----------
q_monthly_all <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
),
Dated AS (
  SELECT RawDate, DATEFROMPARTS(YEAR(RawDate),MONTH(RawDate),1) AS DateBucket
  FROM Base
),
Agg AS (
  SELECT DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b JOIN Dated d ON d.RawDate=b.RawDate
  GROUP BY DateBucket
)
SELECT
  Market='ALL', Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Date;"

q_monthly_country <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD, us.COUNTRY
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT=b.PARTYID
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
    AND us.COUNTRY IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
),
Dated AS (
  SELECT RawDate, COUNTRY AS Market,
         DATEFROMPARTS(YEAR(RawDate),MONTH(RawDate),1) AS DateBucket
  FROM Base
),
Agg AS (
  SELECT d.Market AS Market, DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b
  JOIN Dated d ON d.RawDate=b.RawDate AND d.Market=b.COUNTRY
  GROUP BY d.Market, DateBucket
)
SELECT
  Market, Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Market, Date;"

q_monthly_others <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD, us.COUNTRY
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT=b.PARTYID
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
    AND (us.COUNTRY IS NULL OR us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ'))
),
Dated AS (
  SELECT RawDate, DATEFROMPARTS(YEAR(RawDate),MONTH(RawDate),1) AS DateBucket
  FROM Base
),
Agg AS (
  SELECT DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b JOIN Dated d ON d.RawDate=b.RawDate
  GROUP BY DateBucket
)
SELECT
  Market='Others', Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Date;"

q_monthly_brand <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD,
         CASE WHEN us.BRANDID=1 AND (us.COUNTRY IS NULL OR us.COUNTRY<>'EG') THEN 'GCC'
              WHEN us.BRANDID=6 THEN 'BET' END AS Market
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT=b.PARTYID
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
    AND ((us.BRANDID=1 AND (us.COUNTRY IS NULL OR us.COUNTRY<>'EG')) OR us.BRANDID=6)
),
Dated AS (
  SELECT RawDate, Market, DATEFROMPARTS(YEAR(RawDate),MONTH(RawDate),1) AS DateBucket
  FROM Base
),
Agg AS (
  SELECT d.Market AS Market, DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b
  JOIN Dated d ON d.RawDate=b.RawDate AND d.Market=b.Market
  GROUP BY d.Market, DateBucket
)
SELECT
  Market, Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Market, Date;"

# ---------- MTD (cap at yesterday’s day-of-month) ----------
q_mtd_all <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
),
Dated AS (
  SELECT RawDate, DATEFROMPARTS(YEAR(RawDate),MONTH(RawDate),1) AS MonthStart
  FROM Base
),
Caps AS (
  SELECT MonthStart,
         CASE WHEN DATEADD(DAY,@CutoffDay-1,MonthStart) > EOMONTH(MonthStart)
              THEN EOMONTH(MonthStart)
              ELSE DATEADD(DAY,@CutoffDay-1,MonthStart) END AS CapDate
  FROM (SELECT DISTINCT MonthStart FROM Dated) m
),
Agg AS (
  SELECT d.MonthStart AS DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b
  JOIN Dated d ON d.RawDate=b.RawDate
  JOIN Caps  c ON c.MonthStart=d.MonthStart
  WHERE b.RawDate BETWEEN d.MonthStart AND c.CapDate
  GROUP BY d.MonthStart
)
SELECT
  Market='ALL', Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Date;"

q_mtd_country <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD, us.COUNTRY
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT=b.PARTYID
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
    AND us.COUNTRY IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
),
Dated AS (
  SELECT RawDate, COUNTRY AS Market,
         DATEFROMPARTS(YEAR(RawDate),MONTH(RawDate),1) AS MonthStart
  FROM Base
),
Caps AS (
  SELECT MonthStart,
         CASE WHEN DATEADD(DAY,@CutoffDay-1,MonthStart) > EOMONTH(MonthStart)
              THEN EOMONTH(MonthStart)
              ELSE DATEADD(DAY,@CutoffDay-1,MonthStart) END AS CapDate
  FROM (SELECT DISTINCT MonthStart FROM Dated) m
),
Agg AS (
  SELECT d.Market AS Market, d.MonthStart AS DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b
  JOIN Dated d ON d.RawDate=b.RawDate AND d.Market=b.COUNTRY
  JOIN Caps  c ON c.MonthStart=d.MonthStart
  WHERE b.RawDate BETWEEN d.MonthStart AND c.CapDate
  GROUP BY d.Market, d.MonthStart
)
SELECT
  Market, Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Market, Date;"

q_mtd_others <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD, us.COUNTRY
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT=b.PARTYID
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
    AND (us.COUNTRY IS NULL OR us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ'))
),
Dated AS (
  SELECT RawDate, DATEFROMPARTS(YEAR(RawDate),MONTH(RawDate),1) AS MonthStart
  FROM Base
),
Caps AS (
  SELECT MonthStart,
         CASE WHEN DATEADD(DAY,@CutoffDay-1,MonthStart) > EOMONTH(MonthStart)
              THEN EOMONTH(MonthStart)
              ELSE DATEADD(DAY,@CutoffDay-1,MonthStart) END AS CapDate
  FROM (SELECT DISTINCT MonthStart FROM Dated) m
),
Agg AS (
  SELECT d.MonthStart AS DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b
  JOIN Dated d ON d.RawDate=b.RawDate
  JOIN Caps  c ON c.MonthStart=d.MonthStart
  WHERE b.RawDate BETWEEN d.MonthStart AND c.CapDate
  GROUP BY d.MonthStart
)
SELECT
  Market='Others', Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Date;"

q_mtd_brand <- "
;WITH Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD,
         CASE WHEN us.BRANDID=1 AND (us.COUNTRY IS NULL OR us.COUNTRY<>'EG') THEN 'GCC'
              WHEN us.BRANDID=6 THEN 'BET' END AS Market
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT=b.PARTYID
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
    AND ((us.BRANDID=1 AND (us.COUNTRY IS NULL OR us.COUNTRY<>'EG')) OR us.BRANDID=6)
),
Dated AS (
  SELECT RawDate, Market, DATEFROMPARTS(YEAR(RawDate),MONTH(RawDate),1) AS MonthStart
  FROM Base
),
Caps AS (
  SELECT MonthStart,
         CASE WHEN DATEADD(DAY,@CutoffDay-1,MonthStart) > EOMONTH(MonthStart)
              THEN EOMONTH(MonthStart)
              ELSE DATEADD(DAY,@CutoffDay-1,MonthStart) END AS CapDate
  FROM (SELECT DISTINCT MonthStart FROM Dated) m
),
Agg AS (
  SELECT d.Market AS Market, d.MonthStart AS DateBucket,
         COUNT(DISTINCT CASE WHEN b.RMP=1 THEN b.PARTYID END) AS RMPs,
         COUNT(DISTINCT CASE WHEN b.FTD=1 THEN b.PARTYID END) AS FTDs
  FROM Base b
  JOIN Dated d ON d.RawDate=b.RawDate AND d.Market=b.Market
  JOIN Caps  c ON c.MonthStart=d.MonthStart
  WHERE b.RawDate BETWEEN d.MonthStart AND c.CapDate
  GROUP BY d.Market, d.MonthStart
)
SELECT
  Market, Date=DateBucket, RMPs, FTDs,
  RMPs_prev = LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket),
  Retention = ROUND(CASE WHEN LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) IS NULL
                             OR LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket)=0
                         THEN NULL ELSE 100.0*(RMPs-FTDs)/LAG(RMPs) OVER (PARTITION BY Market ORDER BY DateBucket) END, 2)
FROM Agg
ORDER BY Market, Date;"

header <- switch(period,
                 weekly  = weekly_header,
                 monthly = monthly_header,
                 mtd     = mtd_header)
body <- switch(period,
               weekly  = switch(slice, all=q_weekly_all, country=q_weekly_country, others=q_weekly_others, brand=q_weekly_brand),
               monthly = switch(slice, all=q_monthly_all, country=q_monthly_country, others=q_monthly_others, brand=q_monthly_brand),
               mtd     = switch(slice, all=q_mtd_all, country=q_mtd_country, others=q_mtd_others, brand=q_mtd_brand))

header <- as.character(glue(header, cutoff = cutoff, .open = "{", .close = "}"))
sql <- paste0(header, body)
if (length(sql) != 1L || is.na(sql) || nchar(sql) == 0L) stop("build_sql() produced an empty SQL string.")
sql
    }
    
    # --- helper to run 4 slices and rbind for one period ---
    run_period <- function(period) {
      slices <- c("all","country","others","brand")
      bind_rows(lapply(slices, function(slc) {
        base::message(sprintf("Running %s - %s ...", toupper(period), toupper(slc)))
        sql <- build_sql(period = period, slice = slc, cutoff = cutoff_date)
        res <- DBI::dbGetQuery(conn, sql)
        res %>%
          mutate(Slice = toupper(slc)) %>%
          select(Slice, Market, Date, RMPs, FTDs, RMPs_prev, Retention)
      })) %>%
        mutate(Date = as.Date(Date)) %>%
        arrange(Slice, Market, Date)
    }
    
    # --- produce the three final data frames ---
    df_ret_weekly  <- run_period("weekly")
    df_ret_monthly <- run_period("monthly")
    df_ret_MTD     <- run_period("mtd")
    

    
     
    # Build daily RMP/FTD (include 'ALL' aggregate so it matches df_ret)
    daily <- df %>%
      filter(METRIC %in% c("RMPs","FTDs")) %>%
      group_by(Date, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE, values_fill = 0) %>%
      rename(RMP = RMPs, FTD = FTDs)
    
    df_ret <- daily %>%
      arrange(Market, Date) %>%
      group_by(Market) %>%
      mutate(
        RMP_prev  = lag(RMP),
        Retention = if_else(
          is.na(RMP_prev) | RMP_prev == 0,
          NA_real_,
          round(100 * (RMP - FTD) / RMP_prev, 2)  # percent, 2 decimals
        )
      ) %>%
      transmute(Date, Market, Retention) %>%
      ungroup()
    
    df_ret <- df_ret %>%
      pivot_longer(
        cols = c(Retention),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    df <- rbind(df, df_ret)
    
    # Ensure Date is Date type
    df$Date <- as.Date(df$Date)
    
    # --- WEEKLY ---
    df_weekly <- df %>%
      mutate(Week = floor_date(Date, "week", week_start = 1)) %>%
      filter(METRIC %in% c("DEPs","GGR","STAKE","NGR")) %>%   # exclude Margin, will recalc
      group_by(Week, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Week, Market), names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Week)
    
    
    df_ret_weekly <- df_ret_weekly %>%
      mutate(Date = as.Date(Date)) %>%                 # keep Date as Date (optional)
      pivot_longer(
        cols = c(FTDs, RMPs, Retention),
        names_to = "METRIC",
        values_to = "VALUE",
        values_drop_na = TRUE
      ) %>%
      select(Date, Market, METRIC, VALUE) %>%
      arrange(Date, Market, METRIC)
    
    
    df_weekly <- bind_rows(df_weekly, df_ret_weekly)
    
    # --- MONTHLY ---
    df_monthly <- df %>%
      mutate(Month = floor_date(Date, "month")) %>%
      filter(METRIC %in% c("DEPs","GGR","STAKE","NGR")) %>%   # exclude Margin, will recalc
      group_by(Month, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Month, Market), names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
 
    
    df_ret_monthly <- df_ret_monthly %>%
      mutate(Date = as.Date(Date)) %>%                 # keep Date as Date (optional)
      pivot_longer(
        cols = c(FTDs, RMPs, Retention),
        names_to = "METRIC",
        values_to = "VALUE",
        values_drop_na = TRUE
      ) %>%
      select(Date, Market, METRIC, VALUE) %>%
      arrange(Date, Market, METRIC)
    
    df_monthly <- bind_rows(df_monthly, df_ret_monthly)
    
    # --- MTD ---
    df_MTD <- df %>%
      mutate(Date = as.Date(Date)) %>%
      # keep same metrics you aggregate elsewhere (Margin will be recomputed)
      filter(METRIC %in% c("DEPs", "GGR","STAKE","NGR")) %>%
      # keep only days 1..cutoff_day (day(yesterday)) for every month in the series
      filter(day(Date) <= day(yesterday)) %>%
      # bucket by month
      mutate(Month = floor_date(Date, "month")) %>%
      group_by(Month, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      # recompute Margin; avoid divide-by-zero
      mutate(Margin = round(if_else(STAKE > 0, (GGR / STAKE) * 100, NA_real_), 1)) %>%
      pivot_longer(-c(Month, Market), names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    
    df_ret_MTD <- df_ret_MTD %>%
      mutate(Date = as.Date(Date)) %>%                 # keep Date as Date (optional)
      pivot_longer(
        cols = c(FTDs, RMPs, Retention),
        names_to = "METRIC",
        values_to = "VALUE",
        values_drop_na = TRUE
      ) %>%
      select(Date, Market, METRIC, VALUE) %>%
      arrange(Date, Market, METRIC)
    
    df_MTD <- bind_rows(df_MTD, df_ret_MTD)
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      #Daily
      df_subset <- df[df$Market == mkt & df$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-main-daily_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Weekly
      df_subset <- df_weekly[df_weekly$Market == mkt & df_weekly$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-main-weekly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Monthly
      df_subset <- df_monthly[df_monthly$Market == mkt & df_monthly$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-main-monthly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #MTD
      df_subset <- df_MTD[df_MTD $Market == mkt & df_MTD $METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-main-MTD_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
    }
    
    write_log("Main metrics files created.")
 
    #####################################################################
    ########################## Main differences #########################
    #####################################################################
    
    start_date = '2024-11-01'
    
    agg_period <- function(df, start_date, end_date) {
      df %>%
        filter(Date >= start_date, Date <= end_date, METRIC %in% c("DEPs","GGR","STAKE","NGR")) %>%
        group_by(Market, METRIC) %>%
        summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
        pivot_wider(names_from = METRIC, values_from = VALUE) %>%
        mutate(Margin = if_else(STAKE > 0, (GGR / STAKE) * 100, NA_real_)) %>%
        pivot_longer(-Market, names_to = "METRIC", values_to = "VALUE")
    }
    
    windows_for_period <- function(yesterday, period) {
      end_cur <- as.Date(yesterday)
      if (period == "Yesterday") {
        start_cur <- end_cur
        start_prev <- end_cur - 1L
        end_prev <- start_prev
      } else if (period == "7 days") {
        start_cur <- end_cur - 6L
        end_prev <- start_cur - 1L
        start_prev <- end_prev - 6L
      } else if (period == "30 days") {
        start_cur <- end_cur - 29L
        end_prev <- start_cur - 1L
        start_prev <- end_prev - 29L
      } else if (period == "MTD") {
        start_cur <- floor_date(end_cur, "month")
        X <- as.integer(end_cur - start_cur) + 1L
        start_prev <- start_cur %m-% months(1)
        prev_month_end <- (start_prev %m+% months(1)) - days(1)
        end_prev <- min(start_prev + days(X - 1L), prev_month_end)
      } else stop("Invalid period")
      list(start_cur = start_cur, end_cur = end_cur,
           start_prev = start_prev, end_prev = end_prev)
    }
    
    build_comp_for_period <- function(df, yesterday, period) {
      w <- windows_for_period(yesterday, period)
      cur <- agg_period(df, w$start_cur, w$end_cur) %>% mutate(Period = period)
      prev <- agg_period(df, w$start_prev, w$end_prev) %>%
        mutate(Period = period) %>% rename(Previous = VALUE)
      cur %>%
        left_join(prev, by = c("Market","METRIC","Period")) %>%
        mutate(
          DIFF_ABSOLUTE   = VALUE - Previous,
          DIFF_PERCENTAGE = if_else(is.na(Previous) | Previous == 0, NA_real_,
                                    round((VALUE - Previous)/Previous * 100, 2))
        )
    }
    
    df_comp <- bind_rows(
      build_comp_for_period(df, yesterday, "Yesterday"),
      build_comp_for_period(df, yesterday, "7 days"),
      build_comp_for_period(df, yesterday, "30 days"),
      build_comp_for_period(df, yesterday, "MTD")
    )
    
    
    # =========================
    # Previous windows — ALL
    # =========================
    query <- "
  DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));

  -- Current MTD (1st..yesterday)
  DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
  DECLARE @MTD_End   DATE = @Yesterday;

  -- Previous month analog (length-capped)
  DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
  DECLARE @PrevMTD_End    DATE = CASE
                                   WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                        THEN EOMONTH(@PrevMonthStart)
                                   ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                 END;

  -- Two months ago analog (length-capped)
  DECLARE @Prev2MonthStart DATE = DATEADD(MONTH, -1, @PrevMonthStart);
  DECLARE @Prev2MTD_End    DATE = CASE
                                    WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart) > EOMONTH(@Prev2MonthStart)
                                         THEN EOMONTH(@Prev2MonthStart)
                                    ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart)
                                  END;

  -- Build the *previous* windows and their own priors
  WITH PeriodsPrev AS (
      -- Previous of 'Yesterday' = D-2; prior to that = D-3
      SELECT 'Yesterday' AS Period,
             DATEADD(DAY, -2, @Yesterday) AS StartDate,
             DATEADD(DAY, -2, @Yesterday) AS EndDate,
             DATEADD(DAY, -3, @Yesterday) AS PrevStartDate,
             DATEADD(DAY, -3, @Yesterday) AS PrevEndDate
      UNION ALL
      -- Previous of last 7 days = D-13..D-7; prior = D-20..D-14
      SELECT '7 days',
             DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday),
             DATEADD(DAY, -20, @Yesterday), DATEADD(DAY, -14, @Yesterday)
      UNION ALL
      -- Previous of last 30 days = D-59..D-30; prior = D-89..D-60
      SELECT '30 days',
             DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday),
             DATEADD(DAY, -89, @Yesterday), DATEADD(DAY, -60, @Yesterday)
      UNION ALL
      -- Previous of MTD = prev-month analog; prior = two-months-ago analog
      SELECT 'MTD',
             @PrevMonthStart, @PrevMTD_End,
             @Prev2MonthStart, @Prev2MTD_End
  )
  SELECT
      p.Period,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
  FROM PeriodsPrev AS p
  CROSS APPLY (
      SELECT
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
          COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
  ) AS Curr
  CROSS APPLY (
      SELECT
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
  ) AS Prev;
"
    
    df_ret_previous <- dbGetQuery(conn, query)
    df_ret_previous$Market <- "ALL"
    
    
    # =========================
    # Previous windows — BY MARKET
    # =========================
    query <- "
  DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));

  DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
  DECLARE @MTD_End   DATE = @Yesterday;

  DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
  DECLARE @PrevMTD_End    DATE = CASE
                                   WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                        THEN EOMONTH(@PrevMonthStart)
                                   ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                 END;

  DECLARE @Prev2MonthStart DATE = DATEADD(MONTH, -1, @PrevMonthStart);
  DECLARE @Prev2MTD_End    DATE = CASE
                                    WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart) > EOMONTH(@Prev2MonthStart)
                                         THEN EOMONTH(@Prev2MonthStart)
                                    ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart)
                                  END;

  WITH PeriodsPrev AS (
      SELECT 'Yesterday' AS Period,
             DATEADD(DAY, -2, @Yesterday) AS StartDate,
             DATEADD(DAY, -2, @Yesterday) AS EndDate,
             DATEADD(DAY, -3, @Yesterday) AS PrevStartDate,
             DATEADD(DAY, -3, @Yesterday) AS PrevEndDate
      UNION ALL
      SELECT '7 days',
             DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday),
             DATEADD(DAY, -20, @Yesterday), DATEADD(DAY, -14, @Yesterday)
      UNION ALL
      SELECT '30 days',
             DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday),
             DATEADD(DAY, -89, @Yesterday), DATEADD(DAY, -60, @Yesterday)
      UNION ALL
      SELECT 'MTD',
             @PrevMonthStart, @PrevMTD_End,
             @Prev2MonthStart, @Prev2MTD_End
  )
  SELECT
      Curr.Market AS Market,
      p.Period,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND((1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
  FROM PeriodsPrev AS p
  CROSS APPLY (
      SELECT
          us.COUNTRY AS Market,
          SUM(CASE WHEN b.RMP = 1 THEN 1 ELSE 0 END) AS RMP,
          SUM(CASE WHEN b.FTD = 1 THEN 1 ELSE 0 END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND us.COUNTRY IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
      GROUP BY us.COUNTRY
  ) AS Curr
  OUTER APPLY (
      SELECT
          SUM(CASE WHEN b.RMP = 1 THEN 1 ELSE 0 END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND us.COUNTRY = Curr.Market
  ) AS Prev;
"
    
    df_ret_markets_previous <- dbGetQuery(conn, query)
    
    
    # =========================
    # Previous windows — OTHERS
    # =========================
    query <- "
  DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));

  DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
  DECLARE @MTD_End   DATE = @Yesterday;

  DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
  DECLARE @PrevMTD_End    DATE = CASE
                                   WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                        THEN EOMONTH(@PrevMonthStart)
                                   ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                 END;

  DECLARE @Prev2MonthStart DATE = DATEADD(MONTH, -1, @PrevMonthStart);
  DECLARE @Prev2MTD_End    DATE = CASE
                                    WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart) > EOMONTH(@Prev2MonthStart)
                                         THEN EOMONTH(@Prev2MonthStart)
                                    ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart)
                                  END;

  WITH PeriodsPrev AS (
      SELECT 'Yesterday' AS Period,
             DATEADD(DAY, -2, @Yesterday) AS StartDate,
             DATEADD(DAY, -2, @Yesterday) AS EndDate,
             DATEADD(DAY, -3, @Yesterday) AS PrevStartDate,
             DATEADD(DAY, -3, @Yesterday) AS PrevEndDate
      UNION ALL
      SELECT '7 days',
             DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday),
             DATEADD(DAY, -20, @Yesterday), DATEADD(DAY, -14, @Yesterday)
      UNION ALL
      SELECT '30 days',
             DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday),
             DATEADD(DAY, -89, @Yesterday), DATEADD(DAY, -60, @Yesterday)
      UNION ALL
      SELECT 'MTD',
             @PrevMonthStart, @PrevMTD_End,
             @Prev2MonthStart, @Prev2MTD_End
  )
  SELECT
      p.Period,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
  FROM PeriodsPrev AS p
  CROSS APPLY (
      SELECT
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
          COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
  ) AS Curr
  CROSS APPLY (
      SELECT
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
  ) AS Prev;
"
    
    df_ret_others_previous <- dbGetQuery(conn, query)
    df_ret_others_previous$Market <- "Others"
    
    
    # =========================
    # Previous windows — BRANDS (GCC/BET)
    # =========================
    query <- "
  DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));

  DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
  DECLARE @MTD_End   DATE = @Yesterday;

  DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
  DECLARE @PrevMTD_End    DATE = CASE
                                   WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
                                        THEN EOMONTH(@PrevMonthStart)
                                   ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
                                 END;

  DECLARE @Prev2MonthStart DATE = DATEADD(MONTH, -1, @PrevMonthStart);
  DECLARE @Prev2MTD_End    DATE = CASE
                                    WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart) > EOMONTH(@Prev2MonthStart)
                                         THEN EOMONTH(@Prev2MonthStart)
                                    ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart)
                                  END;

  WITH PeriodsPrev AS (
      SELECT 'Yesterday' AS Period,
             DATEADD(DAY, -2, @Yesterday) AS StartDate,
             DATEADD(DAY, -2, @Yesterday) AS EndDate,
             DATEADD(DAY, -3, @Yesterday) AS PrevStartDate,
             DATEADD(DAY, -3, @Yesterday) AS PrevEndDate
      UNION ALL
      SELECT '7 days',
             DATEADD(DAY, -13, @Yesterday), DATEADD(DAY, -7, @Yesterday),
             DATEADD(DAY, -20, @Yesterday), DATEADD(DAY, -14, @Yesterday)
      UNION ALL
      SELECT '30 days',
             DATEADD(DAY, -59, @Yesterday), DATEADD(DAY, -30, @Yesterday),
             DATEADD(DAY, -89, @Yesterday), DATEADD(DAY, -60, @Yesterday)
      UNION ALL
      SELECT 'MTD',
             @PrevMonthStart, @PrevMTD_End,
             @Prev2MonthStart, @Prev2MTD_End
  )
  SELECT
      p.Period,
      Curr.Market,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
  FROM PeriodsPrev AS p
  CROSS APPLY (
      SELECT
          m.Market,
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
          COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      CROSS APPLY (
          VALUES (
              CASE
                  WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                  WHEN us.BRANDID = 6                           THEN 'BET'
                  ELSE NULL
              END
          )
      ) AS m(Market)
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND m.Market IS NOT NULL
      GROUP BY m.Market
  ) AS Curr
  OUTER APPLY (
      SELECT
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      CROSS APPLY (
          VALUES (
              CASE
                  WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                  WHEN us.BRANDID = 6                           THEN 'BET'
                  ELSE NULL
              END
          )
      ) AS m(Market)
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND m.Market = Curr.Market
  ) AS Prev;
"
    
    df_ret_brands_previous <- dbGetQuery(conn, query)
    
    # =========================
    # Combine
    # =========================
    df_retention_previous <- rbind(
      df_ret_previous,
      df_ret_markets_previous,
      df_ret_others_previous,
      df_ret_brands_previous
    )
    
    
    # 1) Long format for current
    curr_long <- df_retention
    
    # 2) Long format for previous
    prev_long <- df_retention_previous %>%
      mutate(Market = ifelse(is.na(Market) | Market == "", "ALL", Market)) %>%
      pivot_longer(
        cols = c(RMPs, FTDs, Retention),
        names_to = "METRIC",
        values_to = "Previous"
      )
    
    # 3) Join & compute diffs
    df_comp_retention <- curr_long %>%
      full_join(prev_long, by = c("Market", "Period", "METRIC")) %>%
      mutate(
        DIFF_ABSOLUTE    = VALUE - Previous,
        DIFF_PERCENTAGE  = ifelse(is.na(Previous) | Previous == 0,
                                  NA_real_,
                                  round(((VALUE - Previous) / Previous) * 100, 2))
      ) %>%
      select(Market, METRIC, VALUE, Period, Previous, DIFF_ABSOLUTE, DIFF_PERCENTAGE) %>%
      arrange(Period, Market, METRIC)
    
    
    #Join
    df_main_final <- rbind(df_comp, df_comp_retention)
    
    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0(getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(), "_ai-summary-differences_", mkt, ".csv"),
        row.names = FALSE
      )
    }
    
    write_log("Main differences files created.")
    
    #####################################################################
    ######################## Deposit Group Graph ########################
    #####################################################################    
    
    query <- "

    SELECT
        b.[Date],
        
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
      END AS FTD_Group,
        
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.DEPOSIT_AMOUNT) - SUM(b.Withdrawal_Amount) AS NET,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    INNER JOIN (SELECT PARTYID, [Date], FTD_Since_Months FROM bi_data.dbo.RetentionUsers WHERE [Date] >= '2024-11-01') AS u
        ON u.PARTYID = b.PARTYID
       AND u.[Date]  = b.[Date]
    WHERE
        b.[Date] >= '2024-11-01'
        AND u.FTD_Since_Months >= 0
        
    GROUP BY
        b.[Date],
        u.FTD_Since_Months,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
      END

    "
    df <- dbGetQuery(conn, query) 
    df$Margin <- round((df$GGR / df$STAKE) * 100, digits = 1)
    df$Market <-"ALL"
    
    
    query <- "
        
        SELECT
        b.[Date],
        us.COUNTRY AS Market,
        
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
      END AS FTD_Group,
        
        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.DEPOSIT_AMOUNT) - SUM(b.Withdrawal_Amount) AS NET,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    INNER JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
    INNER JOIN (SELECT PARTYID, [Date], FTD_Since_Months FROM bi_data.dbo.RetentionUsers WHERE [Date] >= '2024-11-01') AS u
        ON u.PARTYID = b.PARTYID
       AND u.[Date]  = b.[Date]
    WHERE
        b.[Date] >= '2024-11-01'
        AND u.FTD_Since_Months >= 0
        AND us.COUNTRY IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
    GROUP BY
        b.[Date],
        us.COUNTRY,
        u.FTD_Since_Months,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
      END
    "
    df_markets <- dbGetQuery(conn, query)
    df_markets$Margin <- round((df_markets$GGR / df_markets$STAKE) * 100, digits = 1)
 
    
    query <- "

      SELECT
          b.[Date],
          g.FTD_Group,
          SUM(b.DEPOSIT_AMOUNT)                                        AS DEPs,
          SUM(b.DEPOSIT_AMOUNT) - SUM(b.Withdrawal_Amount)             AS NET,
          SUM(b.Total_STAKE)                                           AS STAKE,
          SUM(b.Total_GGR)                                             AS GGR,
          SUM(b.Total_NGR)                                             AS NGR,
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END)       AS RMPs,
          COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END)       AS FTDs
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN (
          SELECT PARTYID, [Date], FTD_Since_Months
          FROM bi_data.dbo.RetentionUsers
          WHERE [Date] >= '2024-11-01'
      ) AS u
        ON u.PARTYID = b.PARTYID
       AND u.[Date]  = b.[Date]
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      CROSS APPLY (
          VALUES (
              CASE 
                WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
                WHEN u.FTD_Since_Months BETWEEN 1  AND 3  THEN '[1-3]'
                WHEN u.FTD_Since_Months BETWEEN 4  AND 12 THEN '[4-12]'
                WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
                WHEN u.FTD_Since_Months > 24              THEN '[> 25]'
                ELSE NULL
              END
          )
      ) AS g(FTD_Group)
      WHERE
          b.[Date] >= '2024-11-01'
          AND u.FTD_Since_Months >= 0
          AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
          AND g.FTD_Group IS NOT NULL
      GROUP BY
          b.[Date],
          g.FTD_Group;

    "
    df_others <- dbGetQuery(conn, query) 
    df_others$Margin <- round((df_others$GGR / df_others$STAKE) * 100, digits = 1)
    df_others$Market <-"Others"
    
    
    query <- "
      SELECT
          b.[Date],
          m.Market,
          g.FTD_Group,
          SUM(b.DEPOSIT_AMOUNT)                                        AS DEPs,
          SUM(b.DEPOSIT_AMOUNT) - SUM(b.Withdrawal_Amount)             AS NET,
          SUM(b.Total_STAKE)                                           AS STAKE,
          SUM(b.Total_GGR)                                             AS GGR,
          SUM(b.Total_NGR)                                             AS NGR,
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END)       AS RMPs,
          COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END)       AS FTDs
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN (
          SELECT PARTYID, [Date], FTD_Since_Months
          FROM bi_data.dbo.RetentionUsers
          WHERE [Date] >= '2024-11-01'
      ) AS u
        ON u.PARTYID = b.PARTYID
       AND u.[Date]  = b.[Date]
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      CROSS APPLY (
          VALUES (
              CASE
                  WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                  WHEN us.BRANDID = 6                           THEN 'BET'
                  ELSE NULL
              END
          )
      ) AS m(Market)
      CROSS APPLY (
          VALUES (
              CASE 
                WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
                WHEN u.FTD_Since_Months BETWEEN 1  AND 3  THEN '[1-3]'
                WHEN u.FTD_Since_Months BETWEEN 4  AND 12 THEN '[4-12]'
                WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
                WHEN u.FTD_Since_Months > 24              THEN '[> 25]'
                ELSE NULL
              END
          )
      ) AS g(FTD_Group)
      WHERE
          b.[Date] >= '2024-11-01'
          AND u.FTD_Since_Months >= 0
          AND m.Market IS NOT NULL
          AND g.FTD_Group IS NOT NULL
      GROUP BY
          b.[Date],
          m.Market,
          g.FTD_Group;

    "
    df_brands <- dbGetQuery(conn, query)
    df_brands$Margin <- round((df_brands$GGR / df_brands$STAKE) * 100, digits = 1)
    
    #All together
    df <- rbind(df, df_markets, df_others, df_brands)
    
    df <- df %>%
      pivot_longer(
        cols = c(DEPs, NET, STAKE, GGR, NGR, Margin, RMPs, FTDs),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, Market, FTD_Group, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    # Round all except margin
    df <- df %>%
      mutate(VALUE = if_else(METRIC != "Margin", round(VALUE, 0), VALUE))
    
    # Retention
    # Build daily RMP/FTD (include 'ALL' aggregate so it matches df_ret)
    daily <- df %>%
      filter(METRIC %in% c("RMPs","FTDs")) %>%
      group_by(Date, Market, FTD_Group, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE, values_fill = 0) %>%
      rename(RMP = RMPs, FTD = FTDs)
    
    df_ret <- daily %>%
      arrange(Market, FTD_Group, Date) %>%
      group_by(Market, FTD_Group) %>%
      mutate(
        RMP_prev  = lag(RMP),
        Retention = if_else(
          is.na(RMP_prev) | RMP_prev == 0,
          NA_real_,
          round(100 * (RMP - FTD) / RMP_prev, 2)  # percent, 2 decimals
        )
      ) %>%
      transmute(Date, Market, FTD_Group, Retention) %>%
      ungroup()
    
    df_ret <- df_ret %>%
      pivot_longer(
        cols = c(Retention),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, Market, FTD_Group, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    df <- rbind(df, df_ret)
    
    # Retention
    
    
    # --- params ---    
    
    cutoff_date <- as.Date("2024-11-01")
    
    # Build one SQL string (no ambiguous joins; only one source CTE for aggregation)
    build_sql <- function(period = c("weekly","monthly","mtd"),
                          slice  = c("all","country","others","brand"),
                          cutoff = cutoff_date) {
      
      period <- match.arg(tolower(period), c("weekly","monthly","mtd"))
      slice  <- match.arg(tolower(slice),  c("all","country","others","brand"))
      cutoff <- format(as.Date(cutoff), "%Y-%m-%d")
      
      header <- glue("
SET NOCOUNT ON;
{if (period == 'weekly') 'SET DATEFIRST 1; -- Monday start' else ''}
DECLARE @Yesterday   date      = DATEADD(DAY, -1, CAST(GETDATE() AS date));
DECLARE @Tomorrow    date      = DATEADD(DAY,  1, @Yesterday);
DECLARE @CutoffDate  date      = '{cutoff}';
DECLARE @CutoffStart datetime2 = CAST(@CutoffDate AS datetime2(0));
DECLARE @EndBound    datetime2 = CAST(@Tomorrow   AS datetime2(0));
{if (period == 'mtd') 'DECLARE @CutoffDay int = DAY(@Yesterday);' else ''}
")
      
      case_ftd_group <- "
CASE 
  WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
  WHEN u.FTD_Since_Months BETWEEN 1  AND 3  THEN '[1-3]'
  WHEN u.FTD_Since_Months BETWEEN 4  AND 12 THEN '[4-12]'
  WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
  WHEN u.FTD_Since_Months > 24              THEN '[> 25]'
  ELSE NULL
END"
      
      if (slice == "all") {
        users_join   <- ""
        market_expr  <- "Market = 'ALL'"
        market_where <- "1 = 1"
      } else if (slice == "country") {
        users_join   <- "JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT = b.PARTYID"
        market_expr  <- "Market = us.COUNTRY"
        market_where <- "us.COUNTRY IN ('AE','BH','EG','JO','KW','QA','SA','NZ')"
      } else if (slice == "others") {
        users_join   <- "JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT = b.PARTYID"
        market_expr  <- "Market = 'Others'"
        market_where <- "(us.COUNTRY IS NULL OR us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ'))"
      } else { # brand
        users_join   <- "JOIN bi_data.dbo.Users us ON us.GL_ACCOUNT = b.PARTYID"
        market_expr  <- "
Market = CASE
           WHEN us.BRANDID = 1 AND (us.COUNTRY IS NULL OR us.COUNTRY <> 'EG') THEN 'GCC'
           WHEN us.BRANDID = 6 THEN 'BET'
           ELSE NULL
         END"
        market_where <- "((us.BRANDID = 1 AND (us.COUNTRY IS NULL OR us.COUNTRY <> 'EG')) OR us.BRANDID = 6)"
      }
      
      bucket_expr <- switch(period,
                            weekly  = "DATEADD(DAY, 1 - DATEPART(WEEKDAY, RawDate), RawDate)",
                            monthly = "DATEFROMPARTS(YEAR(RawDate), MONTH(RawDate), 1)",
                            mtd     = "DATEFROMPARTS(YEAR(RawDate), MONTH(RawDate), 1)"
      )
      
      # ---- Build the main body ----
      # Weekly & Monthly share same pattern; MTD needs fully-qualified MonthStart everywhere.
      if (period %in% c("weekly", "monthly")) {
        bucket_alias <- if (period == "weekly") "DateBucket" else "DateBucket"
        weekly_cut   <- if (period == "weekly") "AND d.DateBucket >= @CutoffDate" else ""
        
        body <- glue("
;WITH RU AS (
  SELECT PARTYID, [Date], FTD_Since_Months
  FROM bi_data.dbo.RetentionUsers
  WHERE [Date] >= @CutoffDate
),
Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
),
Joined AS (
  SELECT
    b.RawDate, b.PARTYID, b.RMP, b.FTD,
    {market_expr},
    FTD_Group = {case_ftd_group}
  FROM Base b
  JOIN RU u ON u.PARTYID = b.PARTYID AND u.[Date] = b.RawDate
  {users_join}
  WHERE u.FTD_Since_Months >= 0 AND {market_where}
),
Dated AS (
  SELECT
    j.RawDate, j.PARTYID, j.RMP, j.FTD, j.Market, j.FTD_Group,
    {bucket_alias} = {bucket_expr}
  FROM Joined j
  WHERE j.FTD_Group IS NOT NULL
)
SELECT
  d.Market,
  d.FTD_Group,
  Date = d.{bucket_alias},
  RMPs = COUNT(DISTINCT CASE WHEN d.RMP = 1 THEN d.PARTYID END),
  FTDs = COUNT(DISTINCT CASE WHEN d.FTD = 1 THEN d.PARTYID END)
FROM Dated d
WHERE 1=1
  {weekly_cut}
GROUP BY d.Market, d.FTD_Group, d.{bucket_alias}
ORDER BY d.Market, d.FTD_Group, d.{bucket_alias};
")
      } else {
        # MTD (use d.MonthStart fully-qualified everywhere)
        body <- glue("
;WITH RU AS (
  SELECT PARTYID, [Date], FTD_Since_Months
  FROM bi_data.dbo.RetentionUsers
  WHERE [Date] >= @CutoffDate
),
Base AS (
  SELECT CAST(b.[Date] AS date) AS RawDate, b.PARTYID, b.RMP, b.FTD
  FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
  WHERE b.[Date] >= @CutoffStart AND b.[Date] < @EndBound
),
Joined AS (
  SELECT
    b.RawDate, b.PARTYID, b.RMP, b.FTD,
    {market_expr},
    FTD_Group = {case_ftd_group}
  FROM Base b
  JOIN RU u ON u.PARTYID = b.PARTYID AND u.[Date] = b.RawDate
  {users_join}
  WHERE u.FTD_Since_Months >= 0 AND {market_where}
),
Dated AS (
  SELECT
    j.RawDate, j.PARTYID, j.RMP, j.FTD, j.Market, j.FTD_Group,
    MonthStart = {bucket_expr}
  FROM Joined j
  WHERE j.FTD_Group IS NOT NULL
),
Caps AS (
  SELECT
    d.MonthStart,
    CapDate =
      CASE WHEN DATEADD(DAY, @CutoffDay - 1, d.MonthStart) > EOMONTH(d.MonthStart)
           THEN EOMONTH(d.MonthStart)
           ELSE DATEADD(DAY, @CutoffDay - 1, d.MonthStart) END
  FROM (SELECT DISTINCT MonthStart FROM Dated) d
)
SELECT
  d.Market,
  d.FTD_Group,
  Date = d.MonthStart,
  RMPs = COUNT(DISTINCT CASE WHEN d.RMP = 1 THEN d.PARTYID END),
  FTDs = COUNT(DISTINCT CASE WHEN d.FTD = 1 THEN d.PARTYID END)
FROM Dated d
JOIN Caps  c ON c.MonthStart = d.MonthStart
WHERE d.RawDate BETWEEN d.MonthStart AND c.CapDate
GROUP BY d.Market, d.FTD_Group, d.MonthStart
ORDER BY d.Market, d.FTD_Group, d.MonthStart;
")
      }
      
      sql <- paste0(header, body)
      if (length(sql) != 1L || is.na(sql) || nchar(sql) == 0L) stop("build_sql() produced an empty SQL string.")
      sql
    }
    
    
    # Runner (note: gmailr masks message(); force base::message)
    run_period <- function(period) {
      slices <- c("all","country","others","brand")
      bind_rows(lapply(slices, function(slc) {
        base::message(sprintf("Running %s - %s ...", toupper(period), toupper(slc)))
        sql <- build_sql(period = period, slice = slc, cutoff = cutoff_date)
        DBI::dbGetQuery(conn, sql) %>%
          mutate(Slice = toupper(slc),
                 Date  = as.Date(Date))
      })) %>%
        group_by(Slice, Market, FTD_Group) %>%
        arrange(Date, .by_group = TRUE) %>%
        mutate(
          RMPs_prev = dplyr::lag(RMPs),
          Retention = ifelse(is.na(RMPs_prev) | RMPs_prev == 0,
                             NA_real_,
                             round(100 * (RMPs - FTDs) / RMPs_prev, 2))
        ) %>%
        ungroup() %>%
        select(Market, FTD_Group, Date, RMPs, FTDs,  Retention) %>%
        arrange(Market, FTD_Group, Date)
    }
    
    # Build the three data frames
    df_ret_weekly  <- run_period("weekly")
    df_ret_monthly <- run_period("monthly")
    df_ret_MTD     <- run_period("mtd")
    
    df_ret_weekly <- df_ret_weekly %>%
      group_by(Date, Market, FTD_Group) %>%
      pivot_longer(
        cols = c(RMPs, FTDs, Retention),
        names_to = "METRIC",
        values_to = "VALUE"
      )
    
    df_ret_monthly <- df_ret_monthly %>%
      group_by(Date, Market, FTD_Group) %>%
      pivot_longer(
        cols = c(RMPs, FTDs, Retention),
        names_to = "METRIC",
        values_to = "VALUE"
      )
    
    df_ret_MTD <- df_ret_MTD %>%
      group_by(Date, Market, FTD_Group) %>%
      pivot_longer(
        cols = c(RMPs, FTDs, Retention),
        names_to = "METRIC",
        values_to = "VALUE"
      )
    
    # Ensure Date is Date type
    df$Date <- as.Date(df$Date)
    
    # --- WEEKLY ---
    df_weekly <- df %>%
      mutate(Week = floor_date(Date, "week", week_start = 1)) %>%
      # exclude Margin, recalc later
      filter(METRIC %in% c("DEPs","FTDs","GGR","STAKE","NGR","RMPs")) %>% 
      group_by(Week, Market, FTD_Group, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Week, Market, FTD_Group),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Week)
 
    
    df_weekly <- bind_rows(df_weekly, df_ret_weekly)
    
    # --- MONTHLY ---
    df_monthly <- df %>%
      mutate(Month = floor_date(Date, "month")) %>%
      filter(METRIC %in% c("DEPs","FTDs","GGR","STAKE","NGR","RMPs")) %>% 
      group_by(Month, Market, FTD_Group, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Month, Market, FTD_Group),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    df_monthly <- bind_rows(df_monthly, df_ret_monthly)
    
    # --- MTD ---
    df_MTD <- df %>%
      filter(METRIC %in% c("DEPs","FTDs","GGR","STAKE","NGR","RMPs"),
             day(Date) <= day(yesterday)) %>%
      mutate(Month = floor_date(Date, "month")) %>%
      group_by(Month, Market, FTD_Group, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round(if_else(STAKE > 0, (GGR / STAKE) * 100, NA_real_), 1)) %>%
      pivot_longer(-c(Month, Market, FTD_Group),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    
    df_MTD <- bind_rows(df_MTD, df_ret_MTD)
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      #Daily
      df_subset <- df[df$Market == mkt & df$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-groups-daily_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Weekly
      df_subset <- df_weekly[df_weekly$Market == mkt & df_weekly$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-groups-weekly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Monthly
      df_subset <- df_monthly[df_monthly$Market == mkt & df_monthly$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-groups-monthly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #MTD
      df_subset <- df_MTD[df_MTD $Market == mkt & df_MTD $METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-groups-MTD_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
    }
    
    write_log("Deposit groups files created.")
    
    #####################################################################
    ##################### Deposit group main metrics ####################
    #####################################################################
    
    # Ensure Date is a proper Date type
    df$Date <- as.Date(df$Date)
    
    # Define "yesterday" as the max date in df
    yesterday <- max(df$Date, na.rm = TRUE)
    
    # --- MTD helpers (current month through yesterday vs same days last month) ---
    mtd_start <- as.Date(format(yesterday, "%Y-%m-01"))
    prev_month_start <- as.Date(format(mtd_start - 1, "%Y-%m-01"))
    # Number of days in the current MTD window (0-based offset from the 1st)
    mtd_offset_days <- as.integer(yesterday - mtd_start)
    # End of previous month
    prev_eom <- seq(prev_month_start, by = "1 month", length.out = 2)[2] - 1
    # Previous MTD end = same offset days in prev month, capped at prev EOM
    prev_mtd_end <- min(prev_month_start + mtd_offset_days, prev_eom)
    
    # Period windows
    periods <- list(
      "Yesterday" = c(start = yesterday,       end = yesterday,
                      prev_start = yesterday - 1, prev_end = yesterday - 1),
      "7 days"    = c(start = yesterday - 6,   end = yesterday,
                      prev_start = yesterday - 13, prev_end = yesterday - 7),
      "30 days"   = c(start = yesterday - 29,  end = yesterday,
                      prev_start = yesterday - 59, prev_end = yesterday - 30),
      "MTD"       = c(start = mtd_start,       end = yesterday,
                      prev_start = prev_month_start, prev_end = prev_mtd_end)
    )
    
    # Keep only metrics of interest
    df_filtered <- df %>% filter(METRIC %in% c("DEPs",  "NGR", "STAKE", "Margin"))
    
    # Function to calculate sums and compare with previous period
    calc_period <- function(name, dates) {
      current <- df_filtered %>%
        filter(Date >= dates["start"] & Date <= dates["end"]) %>%
        group_by(Market, METRIC, FTD_Group) %>%
        summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      previous <- df_filtered %>%
        filter(Date >= dates["prev_start"] & Date <= dates["prev_end"]) %>%
        group_by(Market, METRIC, FTD_Group) %>%
        summarise(PREVIOUS = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      # Join and calculate diffs
      full_join(current, previous, by = c("Market", "METRIC", "FTD_Group")) %>%
        mutate(
          Period = name,
          DIFF_ABSOLUTE = VALUE - PREVIOUS,
          DIFF_PERCENTAGE = ifelse(PREVIOUS == 0, NA_real_,
                                   round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
        ) %>%
        select(Period, Market, METRIC, FTD_Group, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    }
    
    # Apply to all periods and bind results
    df_main_final <- bind_rows(
      calc_period("Yesterday", periods[["Yesterday"]]),
      calc_period("7 days",    periods[["7 days"]]),
      calc_period("30 days",   periods[["30 days"]]),
      calc_period("MTD",       periods[["MTD"]])
    )
    
    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0(getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(), "_ai-summary-groups_", mkt, ".csv"),
        row.names = FALSE
      )
    }
    
    write_log("Deposit groups summary files created.")
    
    #####################################################################
    ########################### VVIP Analysis ###########################
    #####################################################################   
    
    query <- "

    SELECT
        b.[Date],
        b.PARTYID,

        SUM(b.DEPOSIT_AMOUNT)    AS DEPs,
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        SUM(b.Total_NGR)         AS NGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b

    INNER JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= '2024-11-01' 
        AND us.VIP_STATUS = 6
        
    GROUP BY
        b.[Date],
        b.PARTYID
    "
    df <- dbGetQuery(conn, query)
    
    query <- "

    SELECT
        u.USERID,
        u.GL_ACCOUNT AS PARTYID,
        CONVERT(date, u.REG_DATE) AS REG_DATE,
        CONVERT(date, fd.FTD_DATE) AS FTD_DATE,
        u.COUNTRY,
        CASE
            WHEN a.SEO_Bool = 1        THEN CONCAT('Aff SEO (', a.BTAG, ')')
            WHEN a.Affiliates_Bool = 1 THEN CONCAT('Aff Social (', a.BTAG, ')')
            WHEN m.Media_Bool = 1      THEN CONCAT('Media (', m.PNID, ')')
            ELSE 'Organic'
        END AS Source,
        CASE
            WHEN u.BRANDID = 1 THEN 'YYY'
            WHEN u.BRANDID = 2 THEN 'YYY EN'
            WHEN u.BRANDID = 3 THEN 'Y88'
            WHEN u.BRANDID = 6 THEN 'BetJordan'
            ELSE 'YYY GLOBAL'
        END AS BRAND
    FROM bi_data.dbo.USERS AS u
    LEFT JOIN (
      SELECT DISTINCT 
        PARTYID, 
        PNID,
        1 AS Media_Bool,
        Source AS SourceMedia 
      FROM bi_data.dbo.MediaCost_NRT ) m ON m.PARTYID = u.GL_ACCOUNT
    LEFT JOIN (  
      SELECT DISTINCT
          af.PARTYID,
          af.BTAG,
          1 AS Affiliates_Bool,
          CASE WHEN d.Source = 'SEO' THEN 1 ELSE 0 END AS SEO_Bool
      FROM bi_data.dbo.AffiliatesCost_NRT AS af
      LEFT JOIN (SELECT DISTINCT BTAG, Source FROM bi_data.dbo.AffiliatesDIC_NRT) AS d
          ON d.BTAG = af.BTAG
  ) a
        ON a.PARTYID = u.GL_ACCOUNT
    LEFT JOIN bi_data.dbo.V_FTD_DATE AS fd
        ON fd.PARTYID = u.GL_ACCOUNT
    WHERE u.VIP_STATUS = 6;
    "
    df_users_vvips <- dbGetQuery(conn, query)
    
    df <- df %>% mutate(Date = as_date(Date))
    
    # ---- Aggregation helper ----
    agg_block <- function(data) {
      summarise(
        data,
        DEPs  = sum(DEPs,  na.rm = TRUE),
        STAKE = sum(STAKE, na.rm = TRUE),
        GGR   = sum(GGR,   na.rm = TRUE),
        NGR   = sum(NGR,   na.rm = TRUE),
        RMPs  = max(RMPs,  na.rm = TRUE)
      )
    }
    
    # ---- Period definitions ----
    periods <- list(
      YESTERDAY = expr(Date == yesterday),
      PREV_DAY  = expr(Date == (yesterday - days(1))),
      LAST_7D   = expr(Date >= (yesterday - days(6)) & Date <= yesterday),
      PREV_7D   = expr(Date >= (yesterday - days(13)) & Date <= (yesterday - days(7))),
      LAST_30D  = expr(Date >= (yesterday - days(29)) & Date <= yesterday),
      PREV_30D  = expr(Date >= (yesterday - days(59)) & Date <= (yesterday - days(30))),
      MTD  = expr(Date >= mtd_start & Date <= yesterday),
      PMTD = expr({
        prev_month_start <- mtd_start %m-% months(1)
        prev_month_end   <- prev_month_start + days(day(yesterday) - 1)
        Date >= prev_month_start & Date <= prev_month_end
      })
    )
    
    # ---- Compute period aggregations ----
    build_period <- function(name, predicate_expr) {
      df %>%
        filter(!!predicate_expr) %>%
        group_by(PARTYID) %>%
        agg_block() %>%
        ungroup() %>%
        mutate(Period = name)
    }
    
    # ✅ Correct order for imap_dfr: first the predicate, then the name
    pieces <- imap_dfr(periods, ~ build_period(.y, .x))
    
    # ---- Long format + differences ----
    pair_map <- list(
      YESTERDAY = "PREV_DAY",
      LAST_7D   = "PREV_7D",
      LAST_30D  = "PREV_30D",
      MTD       = "PMTD"
    )
    
    long_df <- pieces %>%
      pivot_longer(cols = c(DEPs, STAKE, GGR, NGR, RMPs),
                   names_to = "METRIC", values_to = "VALUE")
    
    df_final <- imap_dfr(pair_map, function(prev, curr) {
      current_df  <- filter(long_df, Period == curr) %>%
        mutate(VALUE = coalesce(as.numeric(VALUE), 0))
      
      previous_df <- filter(long_df, Period == prev) %>%
        rename(PREVIOUS = VALUE) %>%
        mutate(PREVIOUS = as.numeric(PREVIOUS)) %>%
        select(PARTYID, METRIC, PREVIOUS)
      
      full_join(current_df, previous_df, by = c("PARTYID", "METRIC")) %>%
        # fill missing after the join
        mutate(
          VALUE    = coalesce(VALUE, 0),
          PREVIOUS = coalesce(PREVIOUS, 0)
        ) %>%
        mutate(
          Period = curr,
          DIFF_ABSOLUTE  = VALUE - PREVIOUS,
          DIFF_PERCENTAGE = ifelse(PREVIOUS == 0, NA_real_,
                                   (VALUE - PREVIOUS) / PREVIOUS * 100)
        ) %>%
        mutate(across(c(VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE), ~ round(.x, 0))) %>%
        # drop rows where both VALUE and PREVIOUS are 0
        filter(!(VALUE == 0 & PREVIOUS == 0))
    })
    
    
    df_final <- df_final %>%
      select(Period, PARTYID, METRIC, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE) %>%
      arrange(PARTYID, Period, METRIC)
    
    # Add player attributes
    df_final <- merge(df_final, df_users_vvips, by = "PARTYID")
    
    df_final_markets <- df_final %>%
      mutate(
        MarketList = pmap(
          list(COUNTRY, BRAND),
          function(country, brand) {
            markets <- c("ALL", country)  # always ALL + the country itself
            
            # Special cases
            if (!is.na(country) && !is.na(brand)) {
              if (country != "EG" && brand == "YYY") markets <- c(markets, "GCC")
              if (brand == "BetJordan") markets <- c(markets, "BET")
              if (!(country %in% c("AE","BH","EG","JO","KW","QA","SA","NZ"))) markets <- c(markets, "Others")
            }
            
            unique(markets)
          }
        )
      ) %>%
      unnest_longer(MarketList, values_to = "Market") %>%
      select(Period, PARTYID, METRIC, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE,
             USERID, REG_DATE, FTD_DATE, COUNTRY, Source, BRAND, Market) %>%
      arrange(PARTYID, Period, METRIC, Market)
    
    df_final_markets <- df_final_markets %>%
      # keep only the four target periods in case others slipped in
      filter(Period %in% c("YESTERDAY", "LAST_7D", "LAST_30D", "MTD")) %>%
      mutate(
        Period = dplyr::recode(
          Period,
          YESTERDAY = "Yesterday",
          LAST_7D   = "7 days",
          LAST_30D  = "30 days",
          MTD       = "MTD",
          .default  = Period
        )
      ) %>%
      select(
        Period, Market, PARTYID, USERID, Source,
        REG_DATE, FTD_DATE, METRIC, VALUE, PREVIOUS,
        DIFF_ABSOLUTE, DIFF_PERCENTAGE
      )
    
    
    # Get unique markets
    markets <- unique(df_final_markets$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_final_markets[df_final_markets$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0(getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(), "_ai-VVIPs-table_", mkt, ".csv"),
        row.names = FALSE
      )
    }
    
    write_log("VVIP files created.")
    
    
    
    #####################################################################
    ############################# VVIP Churn ############################
    #####################################################################   
    
    query <- "

    -- VVIPs with NO RMP=1 and NO deposits > 0 in the last 14 days (excluding today)
    SELECT 
        us.GL_ACCOUNT AS PARTYID,
        CONVERT(date, us.LAST_DEPOSIT_DATE) AS Last_DEP,
        us.LAST_DEPOSIT_AMOUNT AS Last_DEP_VALUE,
        CONVERT(date, us.LAST_WITHDRAW_DATE) AS Last_WD,
        us.LAST_WITHDRAW_AMOUNT AS Last_WD_VALUE
    FROM bi_data.dbo.Users AS us
    WHERE
        us.VIP_STATUS = 6
        AND NOT EXISTS (
            SELECT 1
            FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
            WHERE
                b.PARTYID = us.GL_ACCOUNT
                AND b.[Date] >= DATEADD(day, -14, CAST(GETDATE() AS date))  -- 14 days ago
                AND b.[Date]  < CAST(GETDATE() AS date)                      -- up to yesterday
                AND (b.RMP = 1 OR b.DEPOSIT_AMOUNT > 0)
        );

    "
    df <- dbGetQuery(conn, query)
    
    df_lastbet <- dbGetQuery(conn, "SELECT
                                      v.PARTYID,
                                      CONVERT(date, v.[DATETIME]) AS Last_BET
                                  FROM bi_data.dbo.V_PARTYID_MAX_DATE_BET AS v
                                  INNER JOIN bi_data.dbo.Users AS us
                                      ON us.GL_ACCOUNT = v.PARTYID
                                  WHERE
                                      us.VIP_STATUS = 6"
                             )
    
    
    # Add player attributes
    df <- merge(df_lastbet, df, by = "PARTYID")
    df <- merge(df_users_vvips, df, by = "PARTYID")
    df <- subset(df, !is.na(FTD_DATE))
    
    df <- df %>%
      mutate(
        MarketList = pmap(
          list(COUNTRY, BRAND),
          function(country, brand) {
            markets <- c("ALL", country)  # always ALL + the country itself
            
            # Special cases
            if (!is.na(country) && !is.na(brand)) {
              if (country != "EG" && brand == "YYY") markets <- c(markets, "GCC")
              if (brand == "BetJordan") markets <- c(markets, "BET")
              if (!(country %in% c("AE","BH","EG","JO","KW","QA","SA","NZ"))) markets <- c(markets, "Others")
            }
            
            unique(markets)
          }
        )
      ) %>%
      unnest_longer(MarketList, values_to = "Market")
    df$COUNTRY = NULL
    df$BRAND = NULL
    
    write.csv(
      df,
      paste0(getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(), "_ai-VVIPs-Churn", ".csv"),
      row.names = FALSE
    )
    
    write_log("VVIP churn files created.")
    
    #####################################################################
    ######################### Running campaigns #########################
    #####################################################################   
    
    query <- "

    SELECT
        b.[Date],
        d.PNID,
        SUM(ISNULL(b.Total_NGR, 0)) AS NGR
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY b
    INNER JOIN (
        SELECT DISTINCT PARTYID, PNID
        FROM bi_data.dbo.MediaCost_NRT
    ) AS m
        ON m.PARTYID = b.PARTYID
    INNER JOIN (
        SELECT *
        FROM bi_data.dbo.MediaDIC_NRT
        WHERE Active = 1
    ) AS d
        ON d.PNID = m.PNID
    GROUP BY
        b.[Date],
        d.PNID

    "
    df_ngr <- dbGetQuery(conn, query) 
    
    query <- "

    SELECT
        CONVERT(date, us.REG_DATE) AS [Date],
        d.PNID,
        d.Country_Omega AS Market,
        d.Device_Omega AS DEVICE,
        SUM(ISNULL(cost.COST, 0)) AS COST
    FROM bi_data.dbo.Users us
    INNER JOIN (
        SELECT DISTINCT PARTYID, PNID
        FROM bi_data.dbo.MediaCost_NRT
    ) AS m
        ON m.PARTYID = us.GL_ACCOUNT
    INNER JOIN (
        SELECT *
        FROM bi_data.dbo.MediaDIC_NRT
        WHERE Active = 1
    ) AS d
        ON d.PNID = m.PNID
    LEFT JOIN (
        SELECT PARTYID, SUM(CPA) AS COST
        FROM bi_data.dbo.Media_CPA
        GROUP BY PARTYID
    ) AS cost
        ON cost.PARTYID = m.PARTYID
    GROUP BY
        CONVERT(date, us.REG_DATE),
        d.PNID,
        d.Country_Omega,
        d.Device_Omega;

    "
    df_cost <- dbGetQuery(conn, query) 
    
    # --- Step 0: drop PNIDs where COST is 0 for all dates ---
    pnids_keep <- df_cost %>%
      group_by(PNID) %>%
      summarise(total_cost = sum(COST, na.rm = TRUE), .groups = "drop") %>%
      filter(total_cost > 0) %>%
      pull(PNID)
    
    df_cost <- df_cost %>% filter(PNID %in% pnids_keep)
    df_ngr <- df_ngr %>% filter(PNID %in% pnids_keep)
    
    # 1) Make sure Date is Date type in both
    df_cost <- df_cost %>% mutate(Date = as.Date(Date))
    df_ngr  <- df_ngr  %>% mutate(Date = as.Date(Date))
    
    
    # 3) Build a PNID→(Market, DEVICE) lookup (one-to-one as you noted)
    pnid_lookup <- df_cost %>%
      distinct(PNID, Market, DEVICE)
    
    # 4) Build the complete dictionary of (Date, PNID)
    dict <- bind_rows(
      df_cost %>% select(Date, PNID),
      df_ngr  %>% select(Date, PNID)
    ) %>% distinct()
    
    # 5) Join measures and fill missing with 0; attach Market/DEVICE via PNID
    df <- dict %>%
      left_join(df_cost %>% select(Date, PNID, COST), by = c("Date", "PNID")) %>%
      left_join(df_ngr,                                   by = c("Date", "PNID")) %>%
      mutate(
        COST = replace_na(COST, 0),
        NGR  = replace_na(NGR,  0)
      ) %>%
      left_join(pnid_lookup, by = "PNID") %>%
      relocate(Market, DEVICE, .after = PNID) %>%
      arrange(Date, PNID)
    
    # Calculate acc ROI
    df <- df %>%
      group_by(PNID, Market, DEVICE) %>%
      arrange(Date, .by_group = TRUE) %>%
      mutate(
        COST_ACC = cumsum(COST),
        NGR_ACC  = cumsum(NGR),
        ROI = ifelse(COST_ACC > 0,
                     round((NGR_ACC / COST_ACC) * 100, 0),
                     NA_real_)
      ) %>%
      ungroup()
    
    # We are not interested in the ACCs anymore
    df <- df %>%
      pivot_longer(
        cols = c("COST", "NGR", "ROI" ),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, PNID, Market, DEVICE, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    
    # Round all except margin
    df <- df %>%
      mutate(VALUE = if_else(METRIC != "Margin", round(VALUE, 0), VALUE))
    
    # Ensure Date is Date type
    df$Date <- as.Date(df$Date)
    
    # --- WEEKLY ---
    df_weekly <- df %>%
      mutate(Week = floor_date(Date, "week", week_start = 1)) %>%
      filter(METRIC %in% c("COST", "NGR")) %>%
      group_by(Week, PNID, Market, DEVICE, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      rename(Date = Week)
    
    # --- MONTHLY ---
    df_monthly <- df %>%
      mutate(Month = floor_date(Date, "month")) %>%
      filter(METRIC %in% c("COST", "NGR")) %>%
      group_by(Month, PNID, Market, DEVICE, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      rename(Date = Month)
    
    # --- MTD ---
    df_MTD <- df %>%
      filter(METRIC %in% c("COST","NGR"), day(Date) <= day(yesterday)) %>%
      mutate(Month = floor_date(Date, "month")) %>%
      group_by(Month, PNID, Market, DEVICE, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      rename(Date = Month)
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      #Daily
      df_subset <- df[df$Market == mkt & df$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-running-daily_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
    }
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df_weekly[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      
      #Weekly
      df_subset <- df_weekly[df_weekly$Market == mkt & df_weekly$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-running-weekly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Monthly
      df_subset <- df_monthly[df_monthly$Market == mkt & df_monthly$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-running-monthly-", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #MTD
      df_subset <- df_MTD[df_MTD $Market == mkt & df_MTD $METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-running-MTD_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
    }
    
    write_log("Running campaigns files created.")

    
    
    #####################################################################
    ########################## Running metrics ##########################
    ##################################################################### 
    
    # Ensure Date is a proper Date type
    df$Date <- as.Date(df$Date)
    
    # Define yesterday (max date in your df)
    yesterday <- max(df$Date, na.rm = TRUE)
    
    # --- MTD helpers (1st of current month -> yesterday vs same #days last month, capped at prev EOM)
    mtd_start <- as.Date(format(yesterday, "%Y-%m-01"))
    prev_month_start <- as.Date(format(mtd_start - 1, "%Y-%m-01"))
    mtd_offset_days <- as.integer(yesterday - mtd_start)
    prev_eom <- seq(prev_month_start, by = "1 month", length.out = 2)[2] - 1
    prev_mtd_end <- min(prev_month_start + mtd_offset_days, prev_eom)
    
    # Period windows
    periods <- list(
      "Yesterday" = c(start = yesterday,       end = yesterday,
                      prev_start = yesterday - 1, prev_end = yesterday - 1),
      "7 days"    = c(start = yesterday - 6,   end = yesterday,
                      prev_start = yesterday - 13, prev_end = yesterday - 7),
      "30 days"   = c(start = yesterday - 29,  end = yesterday,
                      prev_start = yesterday - 59, prev_end = yesterday - 30),
      "MTD"       = c(start = mtd_start,       end = yesterday,
                      prev_start = prev_month_start, prev_end = prev_mtd_end)
    )
    
    # Keep only ROI
    df_filtered <- df %>% filter(METRIC == "ROI")
    
    # Function to calculate sums and compare with previous period
    calc_period <- function(name, dates) {
      current <- df_filtered %>%
        filter(Date >= dates["start"] & Date <= dates["end"]) %>%
        group_by(Market, METRIC, PNID, DEVICE) %>%
        summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      previous <- df_filtered %>%
        filter(Date >= dates["prev_start"] & Date <= dates["prev_end"]) %>%
        group_by(Market, METRIC, PNID, DEVICE) %>%
        summarise(PREVIOUS = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      # Join and calculate diffs
      full_join(current, previous, by = c("Market", "METRIC", "PNID", "DEVICE")) %>%
        mutate(
          Period = name,
          DIFF_ABSOLUTE  = VALUE - PREVIOUS,
          DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0,
                                   NA_real_,
                                   round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
        ) %>%
        select(Period, Market, METRIC, PNID, DEVICE,
               VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    }
    
    # Apply to all periods and bind results
    df_main_final <- bind_rows(
      calc_period("Yesterday", periods[["Yesterday"]]),
      calc_period("7 days",    periods[["7 days"]]),
      calc_period("30 days",   periods[["30 days"]]),
      calc_period("MTD",       periods[["MTD"]])
    )
    
    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0(getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(), "_ai-summary-running_", mkt, ".csv"),
        row.names = FALSE
      )
    }
    
    write_log("Running campaigns summary files created.")
    
    #####################################################################
    ########################### Product Graph ###########################
    #####################################################################  
    
    query <- "

    SELECT
        b.[Date],
        
        b.GAME_CATEGORY,
        
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    WHERE
        b.[Date] >= '2024-11-01'

    GROUP BY
        b.[Date],
        b.GAME_CATEGORY

    "
    df <- dbGetQuery(conn, query) 
    df$Margin <- round((df$GGR / df$STAKE) * 100, digits = 1)
    df$Market <-"ALL"
    
    query <- "
        
        SELECT
        b.[Date],
        us.COUNTRY AS Market,
        
        b.GAME_CATEGORY,
        
        SUM(b.Total_STAKE)       AS STAKE,
        SUM(b.Total_GGR)         AS GGR,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    INNER JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
    WHERE
        b.[Date] >= '2024-11-01'
        AND us.COUNTRY IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
    GROUP BY
        b.[Date],
        us.COUNTRY,
        b.GAME_CATEGORY
    "
    df_markets <- dbGetQuery(conn, query)
    df_markets$Margin <- round((df_markets$GGR / df_markets$STAKE) * 100, digits = 1)

    query <- "
        
      SELECT
          b.[Date],
          b.GAME_CATEGORY,
          SUM(b.Total_STAKE)                                     AS STAKE,
          SUM(b.Total_GGR)                                       AS GGR,
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      WHERE
          b.[Date] >= '2024-11-01'
          AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
      GROUP BY
          b.[Date],
          b.GAME_CATEGORY;
    "
    df_others <- dbGetQuery(conn, query)
    df_others$Margin <- round((df_others$GGR / df_others$STAKE) * 100, digits = 1)
    df_others$Market <-"Others"
    
    query <- "

      SELECT
          b.[Date],
          b.GAME_CATEGORY,
          m.Market,
          SUM(b.Total_STAKE)                                     AS STAKE,
          SUM(b.Total_GGR)                                       AS GGR,
          COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMPs
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      CROSS APPLY (
          VALUES (
              CASE
                  WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                  WHEN us.BRANDID = 6                           THEN 'BET'
                  ELSE NULL
              END
          )
      ) AS m(Market)
      WHERE
          b.[Date] >= '2024-11-01'
          AND m.Market IS NOT NULL
      GROUP BY
          b.[Date],
          b.GAME_CATEGORY,
          m.Market;

    "
    df_brands <- dbGetQuery(conn, query) 
    df_brands$Margin <- round((df_brands$GGR / df_brands$STAKE) * 100, digits = 1)
    
    
    # All together
    df <- rbind(df, df_markets, df_others, df_brands)
    
    # Filter out some categories
    df <- df %>%
      filter(
        !is.na(GAME_CATEGORY),
        !GAME_CATEGORY %in% c("Loyalty", "CASHBACK", "OTHER MAN BONUS")
      )
    
    
    df <- df %>%
      pivot_longer(
        cols = c(STAKE, GGR, Margin, RMPs),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, Market, GAME_CATEGORY, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    
    
    # Round all except margin
    df <- df %>%
      mutate(VALUE = if_else(METRIC != "Margin", round(VALUE, 0), VALUE))
    
    # Ensure Date is Date type
    df$Date <- as.Date(df$Date)
    
    # --- WEEKLY ---
    df_weekly <- df %>%
      mutate(Week = floor_date(Date, "week", week_start = 1)) %>%
      # exclude Margin, recalc later
      filter(METRIC %in% c("GGR","STAKE","RMPs")) %>% 
      group_by(Week, Market, GAME_CATEGORY, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Week, Market, GAME_CATEGORY),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Week)
    
    # --- MONTHLY ---
    df_monthly <- df %>%
      mutate(Month = floor_date(Date, "month")) %>%
      filter(METRIC %in% c("GGR","STAKE","RMPs")) %>% 
      group_by(Month, Market, GAME_CATEGORY, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round((GGR / STAKE) * 100, 1)) %>%
      pivot_longer(-c(Month, Market, GAME_CATEGORY),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    # --- MTD ---
    df_MTD <- df %>%
      filter(METRIC %in% c("GGR","STAKE","RMPs"), day(Date) <= day(yesterday)) %>%
      mutate(Month = floor_date(Date, "month")) %>%
      group_by(Month, Market, GAME_CATEGORY, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(names_from = METRIC, values_from = VALUE) %>%
      mutate(Margin = round(if_else(STAKE > 0, (GGR / STAKE) * 100, NA_real_), 1)) %>%
      pivot_longer(-c(Month, Market, GAME_CATEGORY),
                   names_to = "METRIC", values_to = "VALUE") %>%
      rename(Date = Month)
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      #Daily
      df_subset <- df[df$Market == mkt & df$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-product-daily_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Weekly
      df_subset <- df_weekly[df_weekly$Market == mkt & df_weekly$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-product-weekly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      #Monthly
      df_subset <- df_monthly[df_monthly$Market == mkt & df_monthly$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-product-monthly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
      
      #MTD
      df_subset <- df_MTD[df_MTD $Market == mkt & df_MTD $METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-product-MTD_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
    }
    
    write_log("Product files created.")

    #####################################################################
    ######################## Product main metrics #######################
    #####################################################################
    
    # Ensure Date is a proper Date type
    df$Date <- as.Date(df$Date)
    
    # Define yesterday (max date in your df)
    yesterday <- max(df$Date, na.rm = TRUE)
    
    # --- MTD helpers (1st of current month -> yesterday vs same #days last month, capped at prev EOM)
    mtd_start <- as.Date(format(yesterday, "%Y-%m-01"))
    prev_month_start <- as.Date(format(mtd_start - 1, "%Y-%m-01"))
    mtd_offset_days <- as.integer(yesterday - mtd_start)
    prev_eom <- seq(prev_month_start, by = "1 month", length.out = 2)[2] - 1
    prev_mtd_end <- min(prev_month_start + mtd_offset_days, prev_eom)
    
    # Period windows
    periods <- list(
      "Yesterday" = c(start = yesterday,       end = yesterday,
                      prev_start = yesterday - 1, prev_end = yesterday - 1),
      "7 days"    = c(start = yesterday - 6,   end = yesterday,
                      prev_start = yesterday - 13, prev_end = yesterday - 7),
      "30 days"   = c(start = yesterday - 29,  end = yesterday,
                      prev_start = yesterday - 59, prev_end = yesterday - 30),
      "MTD"       = c(start = mtd_start,       end = yesterday,
                      prev_start = prev_month_start, prev_end = prev_mtd_end)
    )
    
    # Keep only metrics of interest
    df_filtered <- df %>% filter(METRIC %in% c("GGR", "RMPs"))
    
    # Function to calculate sums and compare with previous period
    calc_period <- function(name, dates) {
      current <- df_filtered %>%
        filter(Date >= dates["start"] & Date <= dates["end"]) %>%
        group_by(Market, METRIC, GAME_CATEGORY) %>%
        summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      previous <- df_filtered %>%
        filter(Date >= dates["prev_start"] & Date <= dates["prev_end"]) %>%
        group_by(Market, METRIC, GAME_CATEGORY) %>%
        summarise(PREVIOUS = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      full_join(current, previous, by = c("Market", "METRIC", "GAME_CATEGORY")) %>%
        mutate(
          Period = name,
          DIFF_ABSOLUTE  = VALUE - PREVIOUS,
          DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0,
                                   NA_real_,
                                   round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
        ) %>%
        select(Period, Market, METRIC, GAME_CATEGORY, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    }
    
    # Apply to all periods and bind results
    df_comp <- bind_rows(
      calc_period("Yesterday", periods[["Yesterday"]]),
      calc_period("7 days",    periods[["7 days"]]),
      calc_period("30 days",   periods[["30 days"]]),
      calc_period("MTD",       periods[["MTD"]])
    )
    
    # Retention
    
    # =========================
    # HELPERS
    # =========================
    markets_allow <- "('AE','BH','EG','JO','KW','QA','SA','NZ')"  # keep centralized
    
    # =========================
    # CURRENT windows — ALL × FTD_Group
    # =========================
    query <- "
    DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
    
    DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
    DECLARE @MTD_End   DATE = @Yesterday;
    
    DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
    DECLARE @PrevMTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
           THEN EOMONTH(@PrevMonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
    END;
    
    WITH Periods (Period, StartDate, EndDate, PrevStartDate, PrevEndDate) AS (
      SELECT 'Yesterday', @Yesterday, @Yesterday, DATEADD(DAY,-1,@Yesterday), DATEADD(DAY,-1,@Yesterday)
      UNION ALL SELECT '7 days',  DATEADD(DAY,-6,@Yesterday), @Yesterday, DATEADD(DAY,-13,@Yesterday), DATEADD(DAY,-7,@Yesterday)
      UNION ALL SELECT '30 days', DATEADD(DAY,-29,@Yesterday), @Yesterday, DATEADD(DAY,-59,@Yesterday), DATEADD(DAY,-30,@Yesterday)
      UNION ALL SELECT 'MTD',     @MTD_Start, @MTD_End, @PrevMonthStart, @PrevMTD_End
    )
    SELECT
      p.Period,
      Curr.FTD_Group,
      Curr.RMP  AS RMPs,
      Curr.FTD  AS FTDs,
      ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
    FROM Periods AS p
    CROSS APPLY (
      SELECT
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
        END AS FTD_Group,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND u.FTD_Since_Months >= 0
      GROUP BY CASE 
        WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
        WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
        WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
        WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
        WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
        ELSE NULL END
    ) AS Curr
    OUTER APPLY (
      SELECT COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND u.FTD_Since_Months >= 0
        AND CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL END = Curr.FTD_Group
    ) AS Prev;
    "
    df_ret_all <- dbGetQuery(conn, query)
    df_ret_all$Market <- "ALL"

    
    # =========================
    # CURRENT windows — BY MARKET × FTD_Group
    # =========================
    query <- "
    DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
    DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
    DECLARE @MTD_End   DATE = @Yesterday;
    DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
    DECLARE @PrevMTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
           THEN EOMONTH(@PrevMonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
    END;
    
    WITH Periods (Period, StartDate, EndDate, PrevStartDate, PrevEndDate) AS (
      SELECT 'Yesterday', @Yesterday, @Yesterday, DATEADD(DAY,-1,@Yesterday), DATEADD(DAY,-1,@Yesterday)
      UNION ALL SELECT '7 days',  DATEADD(DAY,-6,@Yesterday), @Yesterday, DATEADD(DAY,-13,@Yesterday), DATEADD(DAY,-7,@Yesterday)
      UNION ALL SELECT '30 days', DATEADD(DAY,-29,@Yesterday), @Yesterday, DATEADD(DAY,-59,@Yesterday), DATEADD(DAY,-30,@Yesterday)
      UNION ALL SELECT 'MTD',     @MTD_Start, @MTD_End, @PrevMonthStart, @PrevMTD_End
    )
    SELECT
      Curr.Market,
      p.Period,
      Curr.FTD_Group,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND((1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
    FROM Periods AS p
    CROSS APPLY (
      SELECT
        us.COUNTRY AS Market,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
        END AS FTD_Group,
        SUM(CASE WHEN b.RMP = 1 THEN 1 ELSE 0 END) AS RMP,
        SUM(CASE WHEN b.FTD = 1 THEN 1 ELSE 0 END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND us.COUNTRY IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
        AND u.FTD_Since_Months >= 0
      GROUP BY us.COUNTRY,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL END
    ) AS Curr
    OUTER APPLY (
      SELECT SUM(CASE WHEN b.RMP = 1 THEN 1 ELSE 0 END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND us.COUNTRY = Curr.Market
        AND u.FTD_Since_Months >= 0
        AND CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL END = Curr.FTD_Group
    ) AS Prev;
    "
    df_ret_mkt <- dbGetQuery(conn, query)

    
    # =========================
    # CURRENT windows — OTHERS × FTD_Group
    # =========================
    query <- "
    DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
    
    DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
    DECLARE @MTD_End   DATE = @Yesterday;
    
    DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
    DECLARE @PrevMTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
           THEN EOMONTH(@PrevMonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
    END;
    
    WITH Periods (Period, StartDate, EndDate, PrevStartDate, PrevEndDate) AS (
      SELECT 'Yesterday', @Yesterday, @Yesterday, DATEADD(DAY,-1,@Yesterday), DATEADD(DAY,-1,@Yesterday)
      UNION ALL SELECT '7 days',  DATEADD(DAY,-6,@Yesterday), @Yesterday, DATEADD(DAY,-13,@Yesterday), DATEADD(DAY,-7,@Yesterday)
      UNION ALL SELECT '30 days', DATEADD(DAY,-29,@Yesterday), @Yesterday, DATEADD(DAY,-59,@Yesterday), DATEADD(DAY,-30,@Yesterday)
      UNION ALL SELECT 'MTD',     @MTD_Start, @MTD_End, @PrevMonthStart, @PrevMTD_End
    )
    SELECT
      p.Period,
      Curr.FTD_Group,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND((1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
    FROM Periods AS p
    CROSS APPLY (
      SELECT
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
        END AS FTD_Group,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
        AND u.FTD_Since_Months >= 0
      GROUP BY CASE 
        WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
        WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
        WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
        WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
        WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
        ELSE NULL END
    ) AS Curr
    OUTER APPLY (
      SELECT
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
        AND u.FTD_Since_Months >= 0
        AND CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL END = Curr.FTD_Group
    ) AS Prev;
    "
    
    df_ret_others <- dbGetQuery(conn, query)
    df_ret_others$Market <- "Others"
    
    # =========================
    # CURRENT windows — BRANDS (GCC/BET) × FTD_Group
    # =========================
    query <- "
    DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
    
    -- Current MTD (1st..yesterday)
    DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
    DECLARE @MTD_End   DATE = @Yesterday;
    
    -- Previous-month analog for the MTD denominator (same length, capped to EOM)
    DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
    DECLARE @PrevMTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
           THEN EOMONTH(@PrevMonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
    END;
    
    -- Period windows (current + their immediate priors for the denominator)
    WITH Periods (Period, StartDate, EndDate, PrevStartDate, PrevEndDate) AS (
      SELECT 'Yesterday', @Yesterday, @Yesterday, DATEADD(DAY,-1,@Yesterday), DATEADD(DAY,-1,@Yesterday)
      UNION ALL SELECT '7 days',  DATEADD(DAY,-6,@Yesterday),  @Yesterday, DATEADD(DAY,-13,@Yesterday), DATEADD(DAY,-7,@Yesterday)
      UNION ALL SELECT '30 days', DATEADD(DAY,-29,@Yesterday), @Yesterday, DATEADD(DAY,-59,@Yesterday), DATEADD(DAY,-30,@Yesterday)
      UNION ALL SELECT 'MTD',     @MTD_Start, @MTD_End,        @PrevMonthStart, @PrevMTD_End
    )
    SELECT
      p.Period,
      Curr.Market,
      Curr.FTD_Group,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND((1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
    FROM Periods AS p
    CROSS APPLY (
      SELECT
        m.Market,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
        END AS FTD_Group,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      CROSS APPLY (
          VALUES (
            CASE
              WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
              WHEN us.BRANDID = 6                           THEN 'BET'
              ELSE NULL
            END
          )
      ) AS m(Market)
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND m.Market IS NOT NULL
        AND u.FTD_Since_Months >= 0
      GROUP BY
        m.Market,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
        END
    ) AS Curr
    OUTER APPLY (
      SELECT
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      CROSS APPLY (
          VALUES (
            CASE
              WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
              WHEN us.BRANDID = 6                           THEN 'BET'
              ELSE NULL
            END
          )
      ) AS m(Market)
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND m.Market = Curr.Market
        AND u.FTD_Since_Months >= 0
        AND CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
        END = Curr.FTD_Group
    ) AS Prev;
    "
    
    df_ret_brands <- dbGetQuery(conn, query)
    
    # Combine CURRENT
    df_retention <- rbind(df_ret_all, df_ret_mkt, df_ret_others, df_ret_brands)
    
    
    # =========================
    # PREVIOUS windows — ALL × FTD_Group
    # =========================
    query <- "
    DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
    DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
    DECLARE @MTD_End   DATE = @Yesterday;
    
    DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
    DECLARE @PrevMTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
           THEN EOMONTH(@PrevMonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
    END;
    
    DECLARE @Prev2MonthStart DATE = DATEADD(MONTH, -1, @PrevMonthStart);
    DECLARE @Prev2MTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart) > EOMONTH(@Prev2MonthStart)
           THEN EOMONTH(@Prev2MonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart)
    END;
    
    WITH PeriodsPrev (Period, StartDate, EndDate, PrevStartDate, PrevEndDate) AS (
      SELECT 'Yesterday', DATEADD(DAY,-2,@Yesterday), DATEADD(DAY,-2,@Yesterday), DATEADD(DAY,-3,@Yesterday), DATEADD(DAY,-3,@Yesterday)
      UNION ALL SELECT '7 days',  DATEADD(DAY,-13,@Yesterday), DATEADD(DAY,-7,@Yesterday),  DATEADD(DAY,-20,@Yesterday), DATEADD(DAY,-14,@Yesterday)
      UNION ALL SELECT '30 days', DATEADD(DAY,-59,@Yesterday), DATEADD(DAY,-30,@Yesterday), DATEADD(DAY,-89,@Yesterday), DATEADD(DAY,-60,@Yesterday)
      UNION ALL SELECT 'MTD',     @PrevMonthStart, @PrevMTD_End, @Prev2MonthStart, @Prev2MTD_End
    )
    SELECT
      p.Period,
      Curr.FTD_Group,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND( (1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
    FROM PeriodsPrev AS p
    CROSS APPLY (
      SELECT
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
        END AS FTD_Group,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND u.FTD_Since_Months >= 0
      GROUP BY CASE 
        WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
        WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
        WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
        WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
        WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
        ELSE NULL END
    ) AS Curr
    OUTER APPLY (
      SELECT COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND u.FTD_Since_Months >= 0
        AND CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL END = Curr.FTD_Group
    ) AS Prev;
    "
    df_prev_all <- dbGetQuery(conn, query)
    df_prev_all$Market <- "ALL"
    
    
    # =========================
    # PREVIOUS windows — BY MARKET × FTD_Group
    # =========================
    query <- "
    DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
    DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
    DECLARE @MTD_End   DATE = @Yesterday;
    
    DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
    DECLARE @PrevMTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
           THEN EOMONTH(@PrevMonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
    END;
    
    DECLARE @Prev2MonthStart DATE = DATEADD(MONTH, -1, @PrevMonthStart);
    DECLARE @Prev2MTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart) > EOMONTH(@Prev2MonthStart)
           THEN EOMONTH(@Prev2MonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart)
    END;
    
    WITH PeriodsPrev (Period, StartDate, EndDate, PrevStartDate, PrevEndDate) AS (
      SELECT 'Yesterday', DATEADD(DAY,-2,@Yesterday), DATEADD(DAY,-2,@Yesterday), DATEADD(DAY,-3,@Yesterday), DATEADD(DAY,-3,@Yesterday)
      UNION ALL SELECT '7 days',  DATEADD(DAY,-13,@Yesterday), DATEADD(DAY,-7,@Yesterday),  DATEADD(DAY,-20,@Yesterday), DATEADD(DAY,-14,@Yesterday)
      UNION ALL SELECT '30 days', DATEADD(DAY,-59,@Yesterday), DATEADD(DAY,-30,@Yesterday), DATEADD(DAY,-89,@Yesterday), DATEADD(DAY,-60,@Yesterday)
      UNION ALL SELECT 'MTD',     @PrevMonthStart, @PrevMTD_End, @Prev2MonthStart, @Prev2MTD_End
    )
    SELECT
      Curr.Market,
      p.Period,
      Curr.FTD_Group,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND((1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
    FROM PeriodsPrev AS p
    CROSS APPLY (
      SELECT
        us.COUNTRY AS Market,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
        END AS FTD_Group,
        SUM(CASE WHEN b.RMP = 1 THEN 1 ELSE 0 END) AS RMP,
        SUM(CASE WHEN b.FTD = 1 THEN 1 ELSE 0 END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND us.COUNTRY IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
        AND u.FTD_Since_Months >= 0
      GROUP BY us.COUNTRY,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL END
    ) AS Curr
    OUTER APPLY (
      SELECT
        SUM(CASE WHEN b.RMP = 1 THEN 1 ELSE 0 END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND us.COUNTRY = Curr.Market
        AND u.FTD_Since_Months >= 0
        AND CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL END = Curr.FTD_Group
    ) AS Prev;
    "
    df_prev_mkt <- dbGetQuery(conn, query)
    
    
    # =========================
    # PREVIOUS windows — OTHERS × FTD_Group
    # =========================
    query <- "
    DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
    DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
    DECLARE @MTD_End   DATE = @Yesterday;
    
    DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
    DECLARE @PrevMTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
           THEN EOMONTH(@PrevMonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
    END;
    
    DECLARE @Prev2MonthStart DATE = DATEADD(MONTH, -1, @PrevMonthStart);
    DECLARE @Prev2MTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart) > EOMONTH(@Prev2MonthStart)
           THEN EOMONTH(@Prev2MonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart)
    END;
    
    WITH PeriodsPrev (Period, StartDate, EndDate, PrevStartDate, PrevEndDate) AS (
      SELECT 'Yesterday', DATEADD(DAY,-2,@Yesterday), DATEADD(DAY,-2,@Yesterday), DATEADD(DAY,-3,@Yesterday), DATEADD(DAY,-3,@Yesterday)
      UNION ALL SELECT '7 days',  DATEADD(DAY,-13,@Yesterday), DATEADD(DAY,-7,@Yesterday),  DATEADD(DAY,-20,@Yesterday), DATEADD(DAY,-14,@Yesterday)
      UNION ALL SELECT '30 days', DATEADD(DAY,-59,@Yesterday), DATEADD(DAY,-30,@Yesterday), DATEADD(DAY,-89,@Yesterday), DATEADD(DAY,-60,@Yesterday)
      UNION ALL SELECT 'MTD',     @PrevMonthStart, @PrevMTD_End, @Prev2MonthStart, @Prev2MTD_End
    )
    SELECT
      p.Period,
      Curr.FTD_Group,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND((1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
    FROM PeriodsPrev AS p
    CROSS APPLY (
      SELECT
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
        END AS FTD_Group,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
        AND u.FTD_Since_Months >= 0
      GROUP BY CASE 
        WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
        WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
        WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
        WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
        WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
        ELSE NULL END
    ) AS Curr
    OUTER APPLY (
      SELECT
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
        AND u.FTD_Since_Months >= 0
        AND CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL END = Curr.FTD_Group
    ) AS Prev;
    "
    df_prev_others <- dbGetQuery(conn, query)
    df_prev_others$Market <- "Others"
    
    
    # =========================
    # PREVIOUS windows — BRANDS (GCC/BET) × FTD_Group
    # =========================
    query <- "
    DECLARE @Yesterday DATE = DATEADD(DAY, -1, CAST(GETDATE() AS DATE));
    DECLARE @MTD_Start DATE = DATEFROMPARTS(YEAR(@Yesterday), MONTH(@Yesterday), 1);
    DECLARE @MTD_End   DATE = @Yesterday;
    
    DECLARE @PrevMonthStart DATE = DATEADD(MONTH, -1, @MTD_Start);
    DECLARE @PrevMTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart) > EOMONTH(@PrevMonthStart)
           THEN EOMONTH(@PrevMonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @PrevMonthStart)
    END;
    
    DECLARE @Prev2MonthStart DATE = DATEADD(MONTH, -1, @PrevMonthStart);
    DECLARE @Prev2MTD_End    DATE = CASE
      WHEN DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart) > EOMONTH(@Prev2MonthStart)
           THEN EOMONTH(@Prev2MonthStart)
      ELSE DATEADD(DAY, DATEDIFF(DAY, @MTD_Start, @MTD_End), @Prev2MonthStart)
    END;
    
    WITH PeriodsPrev (Period, StartDate, EndDate, PrevStartDate, PrevEndDate) AS (
      SELECT 'Yesterday', DATEADD(DAY,-2,@Yesterday), DATEADD(DAY,-2,@Yesterday), DATEADD(DAY,-3,@Yesterday), DATEADD(DAY,-3,@Yesterday)
      UNION ALL SELECT '7 days',  DATEADD(DAY,-13,@Yesterday), DATEADD(DAY,-7,@Yesterday),  DATEADD(DAY,-20,@Yesterday), DATEADD(DAY,-14,@Yesterday)
      UNION ALL SELECT '30 days', DATEADD(DAY,-59,@Yesterday), DATEADD(DAY,-30,@Yesterday), DATEADD(DAY,-89,@Yesterday), DATEADD(DAY,-60,@Yesterday)
      UNION ALL SELECT 'MTD',     @PrevMonthStart, @PrevMTD_End, @Prev2MonthStart, @Prev2MTD_End
    )
    SELECT
      p.Period,
      Curr.Market,
      Curr.FTD_Group,
      Curr.RMP AS RMPs,
      Curr.FTD AS FTDs,
      ROUND((1.0 * (Curr.RMP - Curr.FTD) / NULLIF(Prev.RMP, 0)) * 100, 2) AS Retention
    FROM PeriodsPrev AS p
    CROSS APPLY (
      SELECT
        m.Market,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL
        END AS FTD_Group,
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP,
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTD
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      CROSS APPLY (
          VALUES (
              CASE
                  WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                  WHEN us.BRANDID = 6                           THEN 'BET'
                  ELSE NULL
              END
          )
      ) AS m(Market)
      WHERE b.[Date] BETWEEN p.StartDate AND p.EndDate
        AND m.Market IS NOT NULL
        AND u.FTD_Since_Months >= 0
      GROUP BY m.Market,
        CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL END
    ) AS Curr
    OUTER APPLY (
      SELECT
        COUNT(DISTINCT CASE WHEN b.RMP = 1 THEN b.PARTYID END) AS RMP
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      JOIN bi_data.dbo.RetentionUsers AS u
        ON u.PARTYID = b.PARTYID AND u.[Date] = b.[Date]
      CROSS APPLY (
          VALUES (
              CASE
                  WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                  WHEN us.BRANDID = 6                           THEN 'BET'
                  ELSE NULL
              END
          )
      ) AS m(Market)
      WHERE b.[Date] BETWEEN p.PrevStartDate AND p.PrevEndDate
        AND m.Market = Curr.Market
        AND u.FTD_Since_Months >= 0
        AND CASE 
          WHEN u.FTD_Since_Months = 0 THEN '[FTDs]'
          WHEN u.FTD_Since_Months BETWEEN 1 AND 3 THEN '[1-3]'
          WHEN u.FTD_Since_Months BETWEEN 4 AND 12 THEN '[4-12]'
          WHEN u.FTD_Since_Months BETWEEN 13 AND 24 THEN '[13-24]'
          WHEN u.FTD_Since_Months > 24 THEN '[> 25]'
          ELSE NULL END = Curr.FTD_Group
    ) AS Prev;
    "
    df_prev_brands <- dbGetQuery(conn, query)
    
    
    # =========================
    # COMBINE into df_retention_previous
    # =========================
    df_retention_previous <- rbind(
      df_prev_all,
      df_prev_mkt,
      df_prev_others,
      df_prev_brands
    )
    
    
    df_curr <- df_retention
    df_prev <- df_retention_previous
    
    # Long → join → diffs over Market + FTD_Group + Period
    curr_long <- df_curr %>%
      mutate(Market = ifelse(is.na(Market) | Market == "", "ALL", Market),
             FTD_Group = ifelse(is.na(FTD_Group) | FTD_Group == "", "[Unk]", FTD_Group)) %>%
      pivot_longer(cols = c(RMPs, FTDs, Retention), names_to = "METRIC", values_to = "VALUE")
    
    prev_long <- df_prev %>%
      mutate(Market = ifelse(is.na(Market) | Market == "", "ALL", Market),
             FTD_Group = ifelse(is.na(FTD_Group) | FTD_Group == "", "[Unk]", FTD_Group)) %>%
      pivot_longer(cols = c(RMPs, FTDs, Retention), names_to = "METRIC", values_to = "PREVIOUS")
    
    df_comp_retention <- curr_long %>%
      full_join(prev_long, by = c("Market", "FTD_Group", "Period", "METRIC")) %>%
      mutate(
        DIFF_ABSOLUTE   = VALUE - PREVIOUS,
        DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0, NA_real_,
                                 round(((VALUE - PREVIOUS) / PREVIOUS) * 100, 2))
      ) %>%
      select(Market, FTD_Group, METRIC, VALUE, Period, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE) %>%
      arrange(Period, Market, FTD_Group, METRIC)
    
    #Join
    df_main_final <- rbind(df_comp, df_comp_retention)

    
    
    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0(getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(), "_ai-summary-product_", mkt, ".csv"),
        row.names = FALSE
      )        
    }  
    
    write_log("Product summary files created.")
    
    #####################################################################
    ########################## Retention Graph ##########################
    #####################################################################
    
    query <- "

    SELECT
        FORMAT(b.[Date], 'yyyy-MM') AS YearMonth,
        
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs,
        COUNT(DISTINCT CASE WHEN b.REACTIVATED = 1 THEN b.PARTYID END) AS REACTIVATED,
        COUNT(DISTINCT CASE WHEN b.RETAINED = 1 THEN b.PARTYID END) AS RETAINED
    
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    WHERE b.[Date] >= '2025-01-01'
    GROUP BY FORMAT(b.[Date], 'yyyy-MM')

    "
    df <- dbGetQuery(conn, query) 
    df$Market <-"ALL"
    
    query <- "
        
    SELECT
        FORMAT(b.[Date], 'yyyy-MM') AS YearMonth,
        us.COUNTRY AS Market,
        
        COUNT(DISTINCT CASE WHEN b.FTD = 1 THEN b.PARTYID END) AS FTDs,
        COUNT(DISTINCT CASE WHEN b.REACTIVATED = 1 THEN b.PARTYID END) AS REACTIVATED,
        COUNT(DISTINCT CASE WHEN b.RETAINED = 1 THEN b.PARTYID END) AS RETAINED
    
    FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
    INNER JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
    WHERE 
      b.[Date] >= '2025-01-01'
      AND us.COUNTRY IN ('AE', 'BH', 'EG', 'JO', 'KW', 'QA', 'SA', 'NZ')
    GROUP BY 
      FORMAT(b.[Date], 'yyyy-MM'), 
      us.COUNTRY
    
    "
    df_markets <- dbGetQuery(conn, query)
    
    query <- "

      SELECT
          FORMAT(b.[Date], 'yyyy-MM') AS YearMonth,
          COUNT(DISTINCT CASE WHEN b.FTD = 1        THEN b.PARTYID END) AS FTDs,
          COUNT(DISTINCT CASE WHEN b.REACTIVATED = 1 THEN b.PARTYID END) AS REACTIVATED,
          COUNT(DISTINCT CASE WHEN b.RETAINED = 1    THEN b.PARTYID END) AS RETAINED
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      WHERE
          b.[Date] >= '2025-01-01'
          AND us.COUNTRY NOT IN ('AE','BH','EG','JO','KW','QA','SA','NZ')
      GROUP BY
          FORMAT(b.[Date], 'yyyy-MM');

    "
    df_others <- dbGetQuery(conn, query) 
    df_others$Market <-"ALL"
    
    query <- "
        
      SELECT
          FORMAT(b.[Date], 'yyyy-MM') AS YearMonth,
          m.Market,
          COUNT(DISTINCT CASE WHEN b.FTD = 1        THEN b.PARTYID END) AS FTDs,
          COUNT(DISTINCT CASE WHEN b.REACTIVATED = 1 THEN b.PARTYID END) AS REACTIVATED,
          COUNT(DISTINCT CASE WHEN b.RETAINED = 1    THEN b.PARTYID END) AS RETAINED
      FROM V_BI_MASTER_BASE_DAILY_GAME_CATEGORY AS b
      JOIN bi_data.dbo.Users AS us
        ON us.GL_ACCOUNT = b.PARTYID
      CROSS APPLY (
          VALUES (
              CASE
                  WHEN us.BRANDID = 1 AND us.COUNTRY <> 'EG' THEN 'GCC'
                  WHEN us.BRANDID = 6                           THEN 'BET'
                  ELSE NULL
              END
          )
      ) AS m(Market)
      WHERE
          b.[Date] >= '2025-01-01'
          AND m.Market IS NOT NULL
      GROUP BY
          FORMAT(b.[Date], 'yyyy-MM'),
          m.Market;
    
    "
    df_brands <- dbGetQuery(conn, query)
    
    df <- rbind(df, df_markets, df_others, df_brands)
    
    df$Date <- as.Date(paste0(df$YearMonth, "-01"), format = "%Y-%m-%d")
    
    df <- df %>%
      pivot_longer(
        cols = c("FTDs", "RETAINED", "REACTIVATED" ),
        names_to = "METRIC",
        values_to = "VALUE"
      ) %>%
      group_by(Date, Market, METRIC) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    
    # Get unique combinations of Market and METRIC
    combos <- unique(df[, c("Market", "METRIC")])
    
    # Loop through each combination
    for (i in seq_len(nrow(combos))) {
      mkt   <- combos$Market[i]
      metr  <- combos$METRIC[i]
      
      #Daily
      df_subset <- df[df$Market == mkt & df$METRIC == metr, ]
      
      file_name <- paste0(
        getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(),
        "_ai-retention-monthly_", metr, "_", mkt, ".csv"
      )
      write.csv(df_subset, file_name, row.names = FALSE)
      
    }
    
    write_log("Retention files created.")
    
    #####################################################################
    ######################### Retention metrics #########################
    #####################################################################
    
    
    # Round all except margin
    df <- df %>%
      mutate(VALUE = if_else(METRIC != "Margin", round(VALUE, 0), VALUE))
    
    # Ensure Date is Date type
    df$Date <- as.Date(df$Date)
    
    # Keep only relevant metrics
    df_filtered <- df %>% 
      filter(METRIC %in% c("FTDs", "RETAINED", "REACTIVATED"))
    
    # ---------------------------
    # Window-based (Yesterday / 7 days / 30 days / MTD)
    # ---------------------------
    yesterday <- max(df_filtered$Date, na.rm = TRUE)
    
    # MTD helpers
    mtd_start <- as.Date(format(yesterday, "%Y-%m-01"))
    prev_month_start <- as.Date(format(mtd_start - 1, "%Y-%m-01"))
    mtd_offset_days <- as.integer(yesterday - mtd_start)
    prev_eom <- (prev_month_start %m+% months(1)) - days(1)
    prev_mtd_end <- pmin(prev_month_start + mtd_offset_days, prev_eom)
    
    periods <- list(
      "Yesterday" = c(start = yesterday,       end = yesterday,
                      prev_start = yesterday - 1, prev_end = yesterday - 1),
      "7 days"    = c(start = yesterday - 6,   end = yesterday,
                      prev_start = yesterday - 13, prev_end = yesterday - 7),
      "30 days"   = c(start = yesterday - 29,  end = yesterday,
                      prev_start = yesterday - 59, prev_end = yesterday - 30),
      "MTD"       = c(start = mtd_start,       end = yesterday,
                      prev_start = prev_month_start, prev_end = prev_mtd_end)
    )
    
    calc_window <- function(name, dates) {
      current <- df_filtered %>%
        filter(Date >= dates["start"] & Date <= dates["end"]) %>%
        group_by(Market, METRIC) %>%
        summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      previous <- df_filtered %>%
        filter(Date >= dates["prev_start"] & Date <= dates["prev_end"]) %>%
        group_by(Market, METRIC) %>%
        summarise(PREVIOUS = sum(VALUE, na.rm = TRUE), .groups = "drop")
      
      full_join(current, previous, by = c("Market", "METRIC")) %>%
        mutate(
          Period = name,
          DIFF_ABSOLUTE  = VALUE - PREVIOUS,
          DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0,
                                   NA_real_, round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
        ) %>%
        select(Period, Market, METRIC, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    }
    
    df_windows <- bind_rows(
      calc_window("Yesterday", periods[["Yesterday"]]),
      calc_window("7 days",    periods[["7 days"]]),
      calc_window("30 days",   periods[["30 days"]]),
      calc_window("MTD",       periods[["MTD"]])
    )
    
    # ---------------------------
    # Monthly comparisons (MoM and 3MoM)
    # ---------------------------
    # Monthly aggregation
    df_monthly <- df_filtered %>%
      mutate(Month = floor_date(Date, "month")) %>%
      group_by(Market, METRIC, Month) %>%
      summarise(VALUE = sum(VALUE, na.rm = TRUE), .groups = "drop")
    
    current_month <- floor_date(yesterday, "month")
    prev_month    <- current_month %m-% months(1)
    month_minus3  <- current_month %m-% months(3)
    
    # MoM: current month vs previous month
    mom_curr <- df_monthly %>% filter(Month == current_month) %>% select(-Month)
    mom_prev <- df_monthly %>% filter(Month == prev_month)    %>% select(-Month) %>%
      rename(PREVIOUS = VALUE)
    
    df_mom <- full_join(mom_curr, mom_prev, by = c("Market","METRIC")) %>%
      mutate(
        Period = "MoM",
        DIFF_ABSOLUTE  = VALUE - PREVIOUS,
        DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0,
                                 NA_real_, round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
      ) %>%
      select(Period, Market, METRIC, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    
    # 3MoM: current month vs the month 3 months ago
    mom3_curr <- df_monthly %>% filter(Month == current_month) %>% select(-Month)
    mom3_prev <- df_monthly %>% filter(Month == month_minus3)  %>% select(-Month) %>%
      rename(PREVIOUS = VALUE)
    
    df_3mom <- full_join(mom3_curr, mom3_prev, by = c("Market","METRIC")) %>%
      mutate(
        Period = "3MoM",
        DIFF_ABSOLUTE  = VALUE - PREVIOUS,
        DIFF_PERCENTAGE = ifelse(is.na(PREVIOUS) | PREVIOUS == 0,
                                 NA_real_, round((VALUE - PREVIOUS) / PREVIOUS * 100, 2))
      ) %>%
      select(Period, Market, METRIC, VALUE, PREVIOUS, DIFF_ABSOLUTE, DIFF_PERCENTAGE)
    
    # ---------------------------
    # Final output
    # ---------------------------
    df_main_final <- bind_rows(
      df_windows,
      df_mom,
      df_3mom
    ) 
    
    # Get unique markets
    markets <- unique(df_main_final$Market)
    
    # Loop through each Market and save its CSV
    for (mkt in markets) {
      df_Market <- df_main_final[df_main_final$Market == mkt, ]
      
      write.csv(
        df_Market,
        paste0(getwd(), "/AI-Performance/aiPerformanceFiles/", Sys.Date(), "_ai-summary-retention_", mkt, ".csv"),
        row.names = FALSE
      )          
    }
    
    write_log("Retention summary files created.")
    
    
  }, error = function(e) {
    retry_attempts <<- retry_attempts + 1
    write_log(paste("Connection failed. Retry attempt:", retry_attempts, "of", max_retries, "Error:", e$message))
    
    if (retry_attempts < max_retries) {
      Sys.sleep(retry_interval)  # Wait before retrying
    } else {
      write_log(paste("The database is locked or faces a related problem. No action was take:", e$message), level = "ERROR")
      write_log("Maximum retry attempts reached. Unable to connect to the database.", level = "ERROR")
      stop("Maximum retry attempts reached. Exiting script.")
    }
  })
}

write_log(paste0("Finishing at ",Sys.time()))
dbDisconnect(conn)
gc()


