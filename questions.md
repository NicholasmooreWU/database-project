```sql
WITH stock_changes AS (
  SELECT 
    date,
    ticker,
    close,
    searches,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close,
    LAG(searches) OVER (PARTITION BY ticker ORDER BY date) AS prev_searchs
  FROM 
    stock_data
)
SELECT 
  date,ticker,close,prev_close,prev_searchs
FROM 
  stock_changes
WHERE 
  prev_searchs < 25 AND 
  close < prev_close;
  ---Should produce an output window where the close was less than the previous close, as well as having searches less than 25. The output is so long that I decided not to include it for the answer sheet.
```



``` sql
WITH filtered_stocks AS (
  SELECT 
    ticker,
    AVG(close) AS avg_close
  FROM 
    stock_data
  WHERE 
    searches < 50
  GROUP BY 
    ticker
)
SELECT 
  ticker,
  avg_close
FROM 
  filtered_stocks
ORDER BY 
  avg_close ASC
LIMIT 5;
"""AIV	"'-1.8807424179453781"
CIO	"'-1.7007048726081848"
EU	0.11788408709853361053
ELA	0.2387035443197877370333
NVDA	0.73122280725139247943"""

```


```sql
WITH IndustryYearlyTotals AS (
    SELECT 
        ci.industry,
        EXTRACT(YEAR FROM sd.date) AS year,
        SUM(sd.close) AS total_value  
        pro351_final.Stock_Data sd
    JOIN 
        pro351_final.Company_Info ci 
        ON sd.symbol = ci.symbol
    WHERE 
        sd.close IS NOT NULL  
    GROUP BY 
        ci.industry, EXTRACT(YEAR FROM sd.date)
),
IndustryYearlyChanges AS (
    SELECT 
        industry,
        year,
        total_value,
        LAG(total_value) OVER (PARTITION BY industry ORDER BY year) AS previous_year_value,
        (total_value - COALESCE(LAG(total_value) OVER (PARTITION BY industry ORDER BY year), 0)) AS value_change
    FROM 
        IndustryYearlyTotals
)
SELECT 
    industry,
    year,
    value_change
FROM 
    IndustryYearlyChanges
WHERE 
    previous_year_value IS NOT NULL 
ORDER BY 
    value_change DESC
LIMIT 1;
---Transportation & Logistics	2005	7321104012022645
```