-- =========================================
-- SECTION: FUNNEL & MONETIZATION ANALYSIS
-- =========================================
-- =========================================
-- SECTION: FUNNEL & MONETIZATION ANALYSIS
-- =========================================

-- =========================================
-- STEP 1: DATA EXPLORATION & KPI BASELINE
-- =========================================

-- Review available user events to define funnel stages
SELECT DISTINCT event_name 
FROM player_events_clean;

-- Total user base
SELECT COUNT(DISTINCT player_id) AS total_players
FROM players_clean;

-- Total Revenue(USD)
SELECT SUM(price_usd) AS total_revenue FROM purchases_clean
WHERE purchase_status = 'Paid';

-- =========================================
-- STEP 2: MONETIZATION METRICS
-- =========================================

-- ARPU: Revenue per total user
SELECT
ROUND(
    SUM(CASE WHEN purchases_clean.purchase_status = 'Paid' THEN purchases_clean.price_usd ELSE 0 END)
    / COUNT(DISTINCT players_clean.player_id),
2) AS arpu
FROM players_clean
LEFT JOIN purchases_clean
    ON players_clean.player_id = purchases_clean.player_id;

-- ARPPU: Revenue per paying user
SELECT
ROUND(
    SUM(price_usd) / COUNT(DISTINCT player_id),
2) AS arppu
FROM purchases_clean
WHERE purchase_status = 'Paid';

-- =========================================
