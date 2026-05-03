-- =========================================
-- SECTION: FUNNEL & MONETIZATION ANALYSIS
-- =========================================

-- STEP 3: USER FUNNEL CONSTRUCTION
-- =========================================

-- Aggregate unique users at each stage of the player journey
WITH funnel_analysis AS (
    SELECT
        COUNT(DISTINCT CASE WHEN event_name = 'App install' THEN player_id END) AS installs,
        COUNT(DISTINCT CASE WHEN event_name = 'Game open' THEN player_id END) AS game_open,
        COUNT(DISTINCT CASE WHEN event_name = 'Tutorial start' THEN player_id END) AS tutorial_start,
        COUNT(DISTINCT CASE WHEN event_name = 'Tutorial complete' THEN player_id END) AS tutorial_complete,
        COUNT(DISTINCT CASE WHEN event_name = 'First match' THEN player_id END) AS first_match,
        COUNT(DISTINCT CASE WHEN event_name = 'Purchase attempt' THEN player_id END) AS purchase_attempt,
        COUNT(DISTINCT CASE WHEN event_name = 'Purchase success' THEN player_id END) AS purchase_success
    FROM player_events_clean
)

-- =========================================
-- STEP 4: CONVERSION RATE ANALYSIS
-- =========================================

SELECT 
    installs,
    game_open,
    tutorial_start,
    tutorial_complete,
    first_match,
    purchase_attempt,
    purchase_success,

    ROUND(game_open / installs, 2) AS install_to_open_rate,
    ROUND(tutorial_complete / tutorial_start, 2) AS tutorial_completion_rate,
    ROUND(first_match / tutorial_complete, 2) AS onboarding_to_gameplay_rate,
    ROUND(purchase_success / first_match, 2) AS gameplay_to_purchase_rate

FROM funnel_analysis;

-- =========================================
