SELECT
    date(processed_at) AS day,
    COUNT(*) FILTER (WHERE action='view') AS views,
    COUNT(*) FILTER (WHERE action='cart') AS carts,
    COUNT(*) FILTER (WHERE action='purchase') AS purchases
FROM events
GROUP BY 1
ORDER BY 1;
