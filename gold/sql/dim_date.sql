-- dim_date: the calendar. One row per day, the same in every catalog, so both companies join on date_key.
-- Financial year runs April to March (India): FY2026-27 = 1 April 2026 to 31 March 2027.
-- Parameters (config/gold.yml, dim_date): :start_date, :end_date, :fiscal_start_month (4 = April)
SELECT
  CAST(date_format(date, 'yyyyMMdd') AS INT)                                         AS date_key,   -- 20261014
  date,
  year(date)                                                                         AS year,
  quarter(date)                                                                      AS quarter,
  month(date)                                                                        AS month,
  date_format(date, 'MMMM')                                                          AS month_name,
  date_format(date, 'yyyy-MM')                                                       AS year_month,
  dayofmonth(date)                                                                   AS day,
  weekday(date) + 1                                                                  AS day_of_week, -- 1 = Monday ... 7 = Sunday
  date_format(date, 'EEEE')                                                          AS day_name,
  weekofyear(date)                                                                   AS week_of_year,
  weekday(date) >= 5                                                                 AS is_weekend,
  date = last_day(date)                                                              AS is_month_end,
  concat('FY', CAST(fy AS STRING), '-', lpad(CAST((fy + 1) % 100 AS STRING), 2, '0')) AS fiscal_year,
  CAST(floor(((month(date) - :fiscal_start_month + 12) % 12) / 3) + 1 AS INT)        AS fiscal_quarter
FROM (
  SELECT date,
         CASE WHEN month(date) >= :fiscal_start_month THEN year(date) ELSE year(date) - 1 END AS fy   -- year the FY starts in
  FROM (SELECT explode(sequence(CAST(:start_date AS DATE), CAST(:end_date AS DATE), INTERVAL 1 DAY)) AS date)
)
