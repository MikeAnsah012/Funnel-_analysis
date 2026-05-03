-- =========================================
-- SECTION: FUNNEL & MONETIZATION ANALYSIS
-- =========================================

-- STEP 5: REVENUE BY REGION
-- =========================================

-- Evaluate geographic revenue distribution
SELECT 
    region,
    revenue,
    ROUND(revenue / total_revenue, 2) AS revenue_percentage
FROM (
    SELECT 
        p.region,
        SUM(pr.price_usd) AS revenue,
        (SELECT SUM(price_usd) 
         FROM purchases_clean 
         WHERE purchase_status = 'Paid') AS total_revenue
    FROM players_clean p
    JOIN purchases_clean pr 
        ON p.player_id = pr.player_id
    WHERE pr.purchase_status = 'Paid'
    GROUP BY p.region
) AS regional_revenue
ORDER BY revenue DESC;

-- =========================================
-- STEP 6: REVENUE BY ITEM
-- =========================================

-- Identify top-performing in-game purchases
SELECT 
    item_name,
    revenue,
    ROUND(revenue / total_revenue, 2) AS revenue_percentage
FROM (
    SELECT 
        item_name,
        SUM(price_usd) AS revenue,
        (SELECT SUM(price_usd) 
         FROM purchases_clean 
         WHERE purchase_status = 'Paid') AS total_revenue
    FROM purchases_clean
    WHERE purchase_status = 'Paid'
    GROUP BY item_name
) AS item_revenue
ORDER BY revenue DESC;

-- =========================================
-- STEP 7: REVENUE BY PLATFORM
-- =========================================

-- Compare monetization performance across platforms
SELECT 
    platform,
    revenue,
    ROUND(revenue / total_revenue, 2) AS revenue_percentage
FROM (
    SELECT 
        players_clean .platform,
        SUM(purchases_clean.price_usd) AS revenue,
        (SELECT SUM(price_usd) 
         FROM purchases_clean 
         WHERE purchase_status = 'Paid') AS total_revenue
    FROM players_clean 
    JOIN purchases_clean 
        ON players_clean.player_id = purchases_clean.player_id
    WHERE purchases_clean.purchase_status = 'Paid'
    GROUP BY players_clean.platform
) AS platform_revenue
ORDER BY revenue DESC;
