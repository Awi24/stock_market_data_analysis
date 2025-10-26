-- MArket wide analysis that answeres various business questions.
--Q01- Which stocks form the core of market activity? 
--(Identify most liquid stocks by avg. daily traded value).

SELECT
    c.company_name,
    sp.ticker,
    AVG (sp.close_price * sp.volume) AS avg_daily_value
FROM 
    stock_prices  sp
JOIN companies  c ON sp.ticker= c.ticker
GROUP BY 
    c.company_name, sp.ticker
ORDER BY 
    c.company_name, avg_daily_value;
