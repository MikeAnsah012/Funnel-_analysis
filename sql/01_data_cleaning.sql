-- =========================================
-- Project: Gaming Analytics Data Cleaning Pipeline
-- Tables: players_raw, player_events_raw, purchases_raw, game_sessions_raw
-- Purpose: Transform raw gaming datasets into clean, standardized, and analysis-ready tables
-- Author: Michael Ansah-Akrofi
-- =========================================
-- This script performs:
-- 1. Data cleaning and standardization across multiple tables
-- 2. Text normalization (trimming, casing)
-- 3. Categorical value standardization (platform, country, acquisition_channel)
-- 4. Conversion of mixed-format dates into DATETIME
-- 5. Handling of missing and inconsistent values
-- 6. Deduplication using window functions
-- 7. Preparation of analysis-ready tables

-- =========================================
-- SECTION 1: PLAYERS TABLE CLEANING
-- =========================================
-- Inspect raw source tables
SELECT * FROM players_raw;
SELECT * FROM player_events_raw;
SELECT * FROM purchases_raw;
SELECT * FROM game_sessions_raw;

-- Create a working copy so that the raw source table remains unchanged
CREATE TABLE players_copy AS
SELECT * FROM players_raw;

-- Remove unnecessary Personally Identifiable Information not needed for analysis
ALTER TABLE players_copy
DROP COLUMN email;

-- Standardize text formatting by trimming spaces and normalizing capitalization
SELECT * FROM players_copy;
SELECT device_name, TRIM(device_name) FROM players_copy;

SET SQL_SAFE_UPDATES = 0;

UPDATE players_copy
SET device_name = TRIM(device_name);

UPDATE players_copy 
SET skill_segment = CONCAT(UPPER(LEFT(skill_segment,1)), LOWER(SUBSTR(skill_segment,2)));

UPDATE players_copy 
SET age = TRIM(age);

UPDATE players_copy 
SET acquisition_channel = CONCAT(UPPER(LEFT(TRIM(acquisition_channel),1)), LOWER(SUBSTR(TRIM(acquisition_channel),2)));

UPDATE players_copy
SET platform = CONCAT(UPPER(LEFT(TRIM(platform),1)),LOWER(SUBSTR(TRIM(platform),2)));

UPDATE players_copy 
SET region = TRIM(region);

UPDATE players_copy
SET country = CONCAT(UPPER(LEFT(TRIM(country),1)),LOWER(SUBSTR(TRIM(country),2)));

UPDATE players_copy 
SET signup_date = TRIM(signup_date);

UPDATE players_copy
SET player_id = TRIM(player_id);

-- Normalize acquisition channel values into consistent business categories
UPDATE players_copy
SET acquisition_channel =
CASE
    WHEN LOWER(TRIM(acquisition_channel)) IN ('organic','org','direct') THEN 'Organic'
    WHEN LOWER(TRIM(acquisition_channel)) IN ('paid search','paid_search','google ads','ads') THEN 'Paid Search'
    WHEN LOWER(TRIM(acquisition_channel)) IN ('youtube','twitch','creator','streamer','influencer') THEN 'Influencer'
    WHEN LOWER(TRIM(acquisition_channel)) IN ('referral','friend refer','friend') THEN 'Referral'
    WHEN LOWER(TRIM(acquisition_channel)) IN ('app store','appstore','store feature') THEN 'App Store'
    ELSE acquisition_channel
END;

-- Normalize platform values into standard platform groups
UPDATE players_copy 
SET platform = 
CASE
    WHEN LOWER(TRIM(platform)) IN ('ps','ps5','playstation') THEN 'PlayStation'
    WHEN LOWER(TRIM(platform)) IN ('xbox series','xbox series x','xbox') THEN 'Xbox'
    WHEN LOWER(TRIM(platform)) IN ('pc/steam','epic pc','steam','windows','pc') THEN 'PC'
    WHEN LOWER(TRIM(platform)) IN ('ios','android','ios/android','mobile') THEN 'Mobile'
    ELSE platform
END;

-- Normalize country names and abbreviations
UPDATE players_copy
SET country = 
CASE
    WHEN LOWER(TRIM(country)) IN ('u.k','u.k.','uk','gb','united kingdom') THEN 'United Kingdom'
    WHEN LOWER(TRIM(country)) IN ('united states','u.s.a','us','usa','united states of america') THEN 'United States'
    WHEN LOWER(TRIM(country)) IN ('de','deutschland','germany') THEN 'Germany'
    WHEN LOWER(TRIM(country)) IN ('mx','méxico','mexico') THEN 'Mexico'
    WHEN LOWER(TRIM(country)) IN ('in','bharat','india') THEN 'India'
    WHEN LOWER(TRIM(country)) IN ('nippon','nihon','jp','japan') THEN 'Japan'
    WHEN LOWER(TRIM(country)) IN ('es','españa','spain') THEN 'Spain'
    WHEN LOWER(TRIM(country)) IN ('fr','république française','france') THEN 'France'
    WHEN LOWER(TRIM(country)) IN ('au','australia') THEN 'Australia'
    WHEN LOWER(TRIM(country)) IN ('br','brasil','brazil') THEN 'Brazil'
    WHEN LOWER(TRIM(country)) IN ('kr','korea','south korea') THEN 'South Korea'
    WHEN LOWER(TRIM(country)) IN ('ca','canada') THEN 'Canada'
    ELSE country
END;

-- Convert inconsistent signup_date formats into a standardized DATETIME field
ALTER TABLE players_copy
ADD COLUMN clean_signup_date DATETIME;

UPDATE players_copy
SET clean_signup_date = 
CASE
    WHEN TRIM(signup_date) = '' THEN NULL
    WHEN TRIM(signup_date) REGEXP "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
        THEN STR_TO_DATE(TRIM(signup_date), "%Y-%m-%d")
    WHEN TRIM(signup_date) REGEXP "^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$"
        THEN STR_TO_DATE(TRIM(signup_date), "%Y-%m-%d %H:%i:%s")
    WHEN TRIM(signup_date) REGEXP "^[0-9]{4}-[0-9]{2}-[0-9]{2}T"
        THEN STR_TO_DATE(TRIM(signup_date), "%Y-%m-%dT%H:%i:%sZ")
    WHEN TRIM(signup_date) REGEXP "^[0-9]{2}/[0-9]{2}/[0-9]{4}$"
        THEN STR_TO_DATE(TRIM(signup_date), "%m/%d/%Y")
    WHEN TRIM(signup_date) REGEXP "^[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}$"
        THEN STR_TO_DATE(TRIM(signup_date), "%m/%d/%y %H:%i")
    WHEN TRIM(signup_date) REGEXP "^[0-9]{4}/[0-9]{2}/[0-9]{2}$"
        THEN STR_TO_DATE(TRIM(signup_date), "%Y/%m/%d")
    WHEN TRIM(signup_date) REGEXP "^[0-9]{2}-[0-9]{2}-[0-9]{4}$"
        THEN STR_TO_DATE(TRIM(signup_date), "%d-%m-%Y")
    WHEN TRIM(signup_date) REGEXP "^[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}$"
        THEN STR_TO_DATE(TRIM(signup_date), "%d.%m.%Y")
    WHEN TRIM(signup_date) REGEXP "^[0-9]{2}\\.[0-9]{2}\\.[0-9]{4} [0-9]{2}:[0-9]{2}$"
        THEN STR_TO_DATE(TRIM(signup_date), "%d.%m.%Y %H:%i")
    WHEN TRIM(signup_date) REGEXP "^[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}$"
        THEN STR_TO_DATE(TRIM(signup_date), "%Y.%m.%d")
    WHEN TRIM(signup_date) REGEXP "^[A-Za-z]{3} [0-9]{2}, [0-9]{4}$"
        THEN STR_TO_DATE(TRIM(signup_date), "%b %d, %Y")
    WHEN TRIM(signup_date) REGEXP "^[A-Za-z]{3} [0-9]{2}, [0-9]{4} [0-9]{2}:[0-9]{2}[AP]M$"
        THEN STR_TO_DATE(TRIM(signup_date), "%b %d, %Y %h:%i%p")
    ELSE NULL
END;

-- Populate missing region values based on standardized country names
UPDATE players_copy
SET region =
CASE
    WHEN country IN ('United States','Canada') THEN 'NA'
    WHEN country IN ('United Kingdom','France','Germany','Spain') THEN 'EMEA'
    WHEN country IN ('South Korea','Australia','India','Japan') THEN 'APAC'
    WHEN country IN ('Mexico','Brazil') THEN 'LATAM'
    ELSE region
END
WHERE region IS NULL OR region = '';

-- Convert blank strings to NULL for cleaner analysis
UPDATE players_copy
SET
    country = NULLIF(TRIM(country), ''),
    region = NULLIF(TRIM(region), ''),
    platform = NULLIF(TRIM(platform), ''),
    acquisition_channel = NULLIF(TRIM(acquisition_channel), ''),
    age = NULLIF(TRIM(age), ''),
    device_name = NULLIF(TRIM(device_name), '');

UPDATE players_copy 
SET age = NULL
WHERE age = 'unknown';

-- Convert cleaned columns to analysis-ready data types
ALTER TABLE players_copy 
MODIFY COLUMN age INT,
MODIFY COLUMN clean_signup_date DATETIME;

-- Deduplicate records using row_number
CREATE TABLE players_copy2 AS
SELECT *,
ROW_NUMBER() OVER (
    PARTITION BY player_id, clean_signup_date, country, region, platform, acquisition_channel, age, skill_segment, device_name
) AS row_num
FROM players_copy;

-- Remove duplicates
DELETE FROM players_copy2
WHERE row_num > 1;

-- Validation checks
SELECT * FROM players_copy2 WHERE row_num > 1;
SELECT * FROM players_copy2 WHERE player_id IS NULL;
SELECT DISTINCT country FROM players_copy2 ORDER BY country;
SELECT DISTINCT platform FROM players_copy2 ORDER BY platform;

-- Finalize cleaned table
ALTER TABLE players_copy2
DROP COLUMN signup_date;

RENAME TABLE players_copy2 TO players_clean;
-- =========================================
-- SECTION 2: PLAYER EVENTS TABLE CLEANING
-- =========================================

-- Preview raw event data to assess structure and data quality
SELECT * FROM player_events_raw;

-- Create working copy to preserve raw dataset integrity
CREATE TABLE player_events_copy AS
SELECT * FROM player_events_raw;

-- =========================================
-- Standardize identifiers and text fields
-- =========================================

-- Trim whitespace from key identifier columns
UPDATE player_events_copy
SET 
    event_id = TRIM(event_id),
    player_id = TRIM(player_id);

-- Normalize event_name formatting (capitalize + remove inconsistencies)
UPDATE player_events_copy 
SET event_name = CONCAT(UPPER(LEFT(TRIM(event_name),1)), LOWER(SUBSTR(TRIM(event_name),2)));

-- Standardize event_name into consistent categories
UPDATE player_events_copy
SET event_name =
CASE
    WHEN LOWER(TRIM(event_name)) IN ('app_install','app install') THEN 'App install'
    WHEN LOWER(TRIM(event_name)) IN ('game_open','game open') THEN 'Game open'
    WHEN LOWER(TRIM(event_name)) IN ('tutorial_complete','tutorial complete','Tutorial complet') THEN 'Tutorial complete'
    WHEN LOWER(TRIM(event_name)) IN ('tutorial_start','tutorial start') THEN 'Tutorial start'
    WHEN LOWER(TRIM(event_name)) IN ('queue_match','queue match') THEN 'Queue match'
    WHEN LOWER(TRIM(event_name)) IN ('match_complete','match complete') THEN 'Match complete'
    WHEN LOWER(TRIM(event_name)) IN ('season_login','season login') THEN 'Season login'
    WHEN LOWER(TRIM(event_name)) IN ('store_view','store view') THEN 'Store view'
    WHEN LOWER(TRIM(event_name)) IN ('add_payment_method','add payment method') THEN 'Add payment method'
    WHEN LOWER(TRIM(event_name)) IN ('first_match','first match') THEN 'First match'
    WHEN LOWER(TRIM(event_name)) IN ('purchase_attempt','purchase attempt') THEN 'Purchase attempt'
    WHEN LOWER(TRIM(event_name)) IN ('purchase_success','purchase success','purchse_success') THEN 'Purchase success'
    WHEN LOWER(TRIM(event_name)) IN ('friend_invite','friend invite') THEN 'Friend invite'
    ELSE event_name
END;

-- =========================================
-- Clean categorical fields
-- =========================================

-- Standardize event_source formatting and handle missing values
UPDATE player_events_copy
SET event_source = CONCAT(UPPER(LEFT(TRIM(event_source),1)), LOWER(SUBSTR(TRIM(event_source),2)));

UPDATE player_events_copy
SET event_source = NULL 
WHERE event_source = '';

-- Standardize campaign_id formatting and handle missing values
UPDATE player_events_copy
SET campaign_id = CONCAT(UPPER(LEFT(TRIM(campaign_id),1)), LOWER(SUBSTR(TRIM(campaign_id),2)));

UPDATE player_events_copy 
SET campaign_id = NULL
WHERE campaign_id = '';

-- Clean semi-structured metadata column by converting blanks to NULL
UPDATE player_events_copy
SET metadata_json = NULL
WHERE metadata_json = '';

-- =========================================
-- Standardize event timestamps
-- =========================================

-- Create cleaned DATETIME column for event_time
ALTER TABLE player_events_copy
ADD COLUMN clean_event_time DATETIME;

-- Convert mixed-format event_time values into standardized DATETIME
UPDATE player_events_copy
SET clean_event_time =
CASE
    WHEN event_time IS NULL OR TRIM(event_time) = '' THEN NULL

    -- ISO and standard datetime formats
    WHEN TRIM(event_time) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(event_time), '%Y-%m-%d %H:%i:%s')

    WHEN TRIM(event_time) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
        THEN STR_TO_DATE(TRIM(event_time), '%Y-%m-%dT%H:%i:%sZ')

    -- Slash formats
    WHEN TRIM(event_time) REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(event_time), '%m/%d/%y %H:%i')

    WHEN TRIM(event_time) REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
        THEN STR_TO_DATE(TRIM(event_time), '%m/%d/%Y')

    -- Dot formats
    WHEN TRIM(event_time) REGEXP '^[0-9]{2}\\.[0-9]{2}\\.[0-9]{4} [0-9]{2}:[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(event_time), '%d.%m.%Y %H:%i')

    WHEN TRIM(event_time) REGEXP '^[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(event_time), '%Y.%m.%d')

    -- Dash formats
    WHEN TRIM(event_time) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
        THEN STR_TO_DATE(TRIM(event_time), '%d-%m-%Y')

    -- Text-based formats
    WHEN TRIM(event_time) REGEXP '^[A-Za-z]{3} [0-9]{2}, [0-9]{4}$'
        THEN STR_TO_DATE(TRIM(event_time), '%b %d, %Y')

    WHEN TRIM(event_time) REGEXP '^[A-Za-z]{3} [0-9]{2}, [0-9]{4} [0-9]{2}:[0-9]{2}[AP]M$'
        THEN STR_TO_DATE(TRIM(event_time), '%b %d, %Y %h:%i%p')

    ELSE NULL
END;

-- Checks for any duplicates within the cleaned table
SELECT *
FROM
(
SELECT *, 
ROW_NUMBER()OVER(PARTITION BY event_id,player_id,event_time,event_source,campaign_id,metadata_json,clean_event_time) AS row_num
FROM player_events_copy 
) AS a 
WHERE row_num > 1;

RENAME TABLE player_events_copy TO player_events_clean;

-- =========================================
-- SECTION 3: GAME SESSIONS TABLE CLEANING
-- =========================================

-- Preview raw game sessions data
SELECT * 
FROM game_sessions_raw;

-- Create a working copy to preserve the raw source table
CREATE TABLE game_sessions_copy AS
SELECT * 
FROM game_sessions_raw;

-- Trim key identifier fields
UPDATE game_sessions_copy 
SET 
    session_id = TRIM(session_id),
    player_id = TRIM(player_id);

-- =========================================
-- Clean Numeric Session Metrics
-- =========================================

-- Create cleaned numeric columns from text-based session metrics
ALTER TABLE game_sessions_copy 
ADD COLUMN clean_session_minutes DECIMAL(10,2),
ADD COLUMN clean_matches_played INT,
ADD COLUMN clean_wins INT,
ADD COLUMN clean_kills INT;

-- Extract numeric values from session_minutes
UPDATE game_sessions_copy 
SET clean_session_minutes = 
CASE
    WHEN session_minutes IS NULL OR TRIM(session_minutes) = '' THEN NULL
    ELSE CAST(REGEXP_REPLACE(session_minutes, '[^0-9.]', '') AS DECIMAL(10,2))
END;

-- Extract numeric values from matches_played
UPDATE game_sessions_copy
SET clean_matches_played = 
CASE
    WHEN matches_played IS NULL OR TRIM(matches_played) = '' THEN NULL
    ELSE CAST(REGEXP_REPLACE(matches_played, '[^0-9]', '') AS UNSIGNED)
END;

-- Extract numeric values from wins
UPDATE game_sessions_copy
SET clean_wins =
CASE
    WHEN wins IS NULL OR TRIM(wins) = '' THEN NULL
    ELSE CAST(REGEXP_REPLACE(wins, '[^0-9]', '') AS UNSIGNED)
END;

-- Extract numeric values from kills
UPDATE game_sessions_copy
SET clean_kills = 
CASE
    WHEN kills IS NULL OR TRIM(kills) = '' THEN NULL
    ELSE CAST(REGEXP_REPLACE(kills, '[^0-9]', '') AS UNSIGNED)
END;

-- =========================================
-- Standardize Categorical Fields
-- =========================================

-- Standardize disconnect_flag into binary values
-- 1 = player disconnected, 0 = player did not disconnect
UPDATE game_sessions_copy
SET disconnect_flag = 
CASE
    WHEN LOWER(TRIM(disconnect_flag)) IN ('y','yes','true','1') THEN 1
    WHEN LOWER(TRIM(disconnect_flag)) IN ('n','no','false','0') THEN 0
    ELSE NULL
END;

-- Standardize reported platform values
UPDATE game_sessions_copy
SET platform_reported =
CASE
    WHEN LOWER(TRIM(platform_reported)) IN ('steam','pc','pc/steam','windows') THEN 'PC'
    WHEN LOWER(TRIM(platform_reported)) IN ('ios','android','ios/android','mobile') THEN 'Mobile'
    WHEN LOWER(TRIM(platform_reported)) IN ('xbox','xbox series','xbox series x') THEN 'Xbox'
    WHEN LOWER(TRIM(platform_reported)) IN ('ps','ps5','playstation') THEN 'PlayStation'
    ELSE NULL
END;

-- Standardize server region values
UPDATE game_sessions_copy 
SET server_region = 
CASE
    WHEN server_region IS NULL OR TRIM(server_region) = '' THEN NULL
    WHEN LOWER(TRIM(server_region)) IN ('na west','na-west','north america west') THEN 'NA-West'
    WHEN LOWER(TRIM(server_region)) IN ('na east','na-east','north america east') THEN 'NA-East'
    WHEN LOWER(TRIM(server_region)) IN ('eu west','eu-west','europe west') THEN 'EU-West'
    WHEN LOWER(TRIM(server_region)) IN ('eu central','eu-central','europe central') THEN 'EU-Central'
    WHEN LOWER(TRIM(server_region)) IN ('apac','asia pacific','asia-pacific') THEN 'APAC'
    ELSE TRIM(server_region)
END;

-- =========================================
-- Clean Session Start Datetime
-- =========================================

-- Convert inconsistent session_start formats into a standardized DATETIME column
ALTER TABLE game_sessions_copy
ADD COLUMN clean_session_start DATETIME;

UPDATE game_sessions_copy
SET clean_session_start =
CASE
    WHEN session_start IS NULL OR TRIM(session_start) = '' THEN NULL

    -- ISO / standard datetime formats
    WHEN TRIM(session_start) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(session_start), '%Y-%m-%d %H:%i:%s')

    WHEN TRIM(session_start) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
        THEN STR_TO_DATE(TRIM(session_start), '%Y-%m-%dT%H:%i:%sZ')

    -- Slash formats
    WHEN TRIM(session_start) REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(session_start), '%m/%d/%y %H:%i')

    WHEN TRIM(session_start) REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(session_start), '%m/%d/%Y %H:%i')

    WHEN TRIM(session_start) REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
        THEN STR_TO_DATE(TRIM(session_start), '%m/%d/%Y')

    WHEN TRIM(session_start) REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(session_start), '%Y/%m/%d')

    -- Dot formats
    WHEN TRIM(session_start) REGEXP '^[0-9]{2}\\.[0-9]{2}\\.[0-9]{4} [0-9]{2}:[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(session_start), '%d.%m.%Y %H:%i')

    WHEN TRIM(session_start) REGEXP '^[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(session_start), '%Y.%m.%d')

    WHEN TRIM(session_start) REGEXP '^[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}$'
        THEN STR_TO_DATE(TRIM(session_start), '%d.%m.%Y')

    -- Dash formats
    WHEN TRIM(session_start) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
        THEN STR_TO_DATE(TRIM(session_start), '%d-%m-%Y')

    WHEN TRIM(session_start) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(session_start), '%Y-%m-%d')

    -- Month-name formats
    WHEN TRIM(session_start) REGEXP '^[A-Za-z]{3} [0-9]{2}, [0-9]{4}$'
        THEN STR_TO_DATE(TRIM(session_start), '%b %d, %Y')

    WHEN TRIM(session_start) REGEXP '^[A-Za-z]{3} [0-9]{2}, [0-9]{4} [0-9]{2}:[0-9]{2}[AP]M$'
        THEN STR_TO_DATE(TRIM(session_start), '%b %d, %Y %h:%i%p')

    ELSE NULL
END;

-- =========================================
-- Validation Checks
-- =========================================

-- Check cleaned numeric fields
SELECT 
    session_minutes,
    clean_session_minutes,
    matches_played,
    clean_matches_played,
    wins,
    clean_wins,
    kills,
    clean_kills
FROM game_sessions_copy;

-- Check standardized platform values
SELECT DISTINCT platform_reported
FROM game_sessions_copy
ORDER BY platform_reported;

-- Check standardized server region values
SELECT DISTINCT server_region
FROM game_sessions_copy
ORDER BY server_region;

-- Check unparsed session_start values
SELECT DISTINCT session_start
FROM game_sessions_copy
WHERE clean_session_start IS NULL
  AND session_start IS NOT NULL
  AND TRIM(session_start) <> '';
  
-- Check duplicates after cleaning
SELECT * FROM
(
SELECT *,
ROW_NUMBER()OVER(PARTITION BY session_id,player_id) AS row_num
FROM game_sessions_copy
) AS a  
WHERE row_num > 1;

-- Finalize cleaned game sessions table
RENAME TABLE game_sessions_copy TO game_sessions_clean;

-- =========================================
-- SECTION 4: PURCHASES TABLE CLEANING
-- =========================================

-- Create working copy to preserve raw data
CREATE TABLE purchases_raw_copy
SELECT * FROM purchases_raw;

-- =========================================
-- BASIC DATA CLEANING
-- =========================================

-- Trim key identifiers
UPDATE purchases_raw_copy
SET order_id = TRIM(order_id),
    player_id = TRIM(player_id);

-- Handle blank values
UPDATE purchases_raw_copy
SET purchase_status = NULL
WHERE TRIM(purchase_status) = '';

UPDATE purchases_raw_copy
SET coupon_code = NULL
WHERE TRIM(coupon_code) = '';

UPDATE purchases_raw_copy
SET payment_method = NULL
WHERE TRIM(payment_method) = '';

-- Standardize purchase status capitalization
UPDATE purchases_raw_copy
SET purchase_status =
CONCAT(
    UPPER(LEFT(TRIM(purchase_status),1)),
    LOWER(SUBSTR(TRIM(purchase_status),2))
);

-- =========================================
-- STANDARDIZE PRODUCT NAMES
-- =========================================

UPDATE purchases_raw_copy
SET item_name =
CASE
    WHEN LOWER(TRIM(item_name)) = 'gem bundle' THEN 'Gem Bundle'
    WHEN LOWER(TRIM(item_name)) = 'myster box' THEN 'Mystery Box'
    WHEN LOWER(TRIM(item_name)) = 'battle pass' THEN 'Battle Pass'
    ELSE item_name
END;

-- =========================================
-- CLEAN AND CONVERT PURCHASE TIME
-- =========================================

ALTER TABLE purchases_raw_copy
ADD COLUMN clean_purchase_time DATETIME;

UPDATE purchases_raw_copy
SET clean_purchase_time =
CASE
    WHEN purchase_time IS NULL OR TRIM(purchase_time) = '' THEN NULL

    -- Standard datetime formats
    WHEN TRIM(purchase_time) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} '
        THEN STR_TO_DATE(TRIM(purchase_time), '%Y-%m-%d %H:%i:%s')

    WHEN TRIM(purchase_time) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
        THEN STR_TO_DATE(TRIM(purchase_time), '%Y-%m-%dT%H:%i:%sZ')

    -- Slash formats
    WHEN TRIM(purchase_time) REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}'
        THEN STR_TO_DATE(TRIM(purchase_time), '%m/%d/%Y')

    -- Dot formats
    WHEN TRIM(purchase_time) REGEXP '^[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}'
        THEN STR_TO_DATE(TRIM(purchase_time), '%d.%m.%Y')

    -- Text formats
    WHEN TRIM(purchase_time) REGEXP '^[A-Za-z]{3}'
        THEN STR_TO_DATE(TRIM(purchase_time), '%b %d, %Y')

    ELSE NULL
END;

-- =========================================
-- CLEAN PRICE (HANDLE GLOBAL FORMATS)
-- =========================================

ALTER TABLE purchases_raw_copy
ADD COLUMN clean_price_local DECIMAL(10,2);

-- Restore original raw price (important after transformations)
UPDATE purchases_raw_copy p
JOIN purchases_raw r ON p.order_id = r.order_id
SET p.price_local = r.price_local;

-- Extract numeric values from mixed currency formats
UPDATE purchases_raw_copy
SET clean_price_local =
CASE
    WHEN price_local IS NULL OR TRIM(price_local) = '' OR LOWER(TRIM(price_local)) = 'na' THEN NULL

    WHEN LOWER(TRIM(price_local)) = 'free' THEN 0.00

    -- Negative values (refunds)
    WHEN TRIM(price_local) REGEXP '-'
        THEN -1 * CAST(
            REPLACE(
                REPLACE(
                    REGEXP_REPLACE(TRIM(price_local), '[^0-9,\\. ]', ''),
                    ',', '.'
                ),
                ' ',
                '.'
            ) AS DECIMAL(10,2)
        )

    -- Space as decimal separator
    WHEN TRIM(price_local) REGEXP '[0-9]+ [0-9]{2}'
        THEN CAST(
            REPLACE(REGEXP_REPLACE(TRIM(price_local), '[^0-9 ]', ''), ' ', '.')
            AS DECIMAL(10,2)
        )

    -- Comma decimal format
    WHEN TRIM(price_local) REGEXP '[0-9]+,[0-9]{2}'
        THEN CAST(
            REPLACE(REGEXP_REPLACE(TRIM(price_local), '[^0-9,]', ''), ',', '.')
            AS DECIMAL(10,2)
        )

    -- Dot decimal format
    WHEN TRIM(price_local) REGEXP '[0-9]+\\.[0-9]{2}'
        THEN CAST(
            TRIM(TRAILING '.' FROM REGEXP_REPLACE(TRIM(price_local), '[^0-9.]', ''))
            AS DECIMAL(10,2)
        )

    -- Whole numbers
    WHEN TRIM(price_local) REGEXP '[0-9]+'
        THEN CAST(
            REGEXP_REPLACE(TRIM(price_local), '[^0-9]', '')
            AS DECIMAL(10,2)
        )

    ELSE NULL
END;

-- =========================================
-- EXTRACT CURRENCY TYPE
-- =========================================

ALTER TABLE purchases_raw_copy
ADD COLUMN clean_currency_hint VARCHAR(10);

UPDATE purchases_raw_copy
SET clean_currency_hint =
CASE
    WHEN price_local IS NULL OR TRIM(price_local) = '' OR TRIM(price_local) = 'free' THEN NULL

    WHEN LOWER(TRIM(price_local)) LIKE '%gbp%' OR price_local LIKE '%£%' THEN 'GBP'
    WHEN LOWER(TRIM(price_local)) LIKE '%eur%' OR price_local LIKE '%€%' THEN 'EUR'
    WHEN LOWER(TRIM(price_local)) LIKE '%jpy%' OR LOWER(TRIM(price_local)) LIKE '%yen%' OR price_local LIKE '%¥%' THEN 'JPY'
    WHEN LOWER(TRIM(price_local)) LIKE '%inr%' OR price_local LIKE '%₹%' THEN 'INR'
    WHEN LOWER(TRIM(price_local)) LIKE '%aud%' OR LOWER(TRIM(price_local)) LIKE '%a$%' THEN 'AUD'
    WHEN LOWER(TRIM(price_local)) LIKE '%cad%' OR LOWER(TRIM(price_local)) LIKE '%c$%' THEN 'CAD'
    WHEN LOWER(TRIM(price_local)) LIKE '%brl%' OR LOWER(TRIM(price_local)) LIKE '%r$%' THEN 'BRL'
    WHEN LOWER(TRIM(price_local)) LIKE '%usd%' OR price_local LIKE '%$%' THEN 'USD'

    ELSE 'Unknown'
END;

-- =========================================
-- CONVERT TO USD (NORMALIZATION)
-- =========================================

ALTER TABLE purchases_raw_copy
ADD COLUMN price_usd DECIMAL(10,2);

UPDATE purchases_raw_copy
SET price_usd =
CASE 
    WHEN clean_price_local IS NULL THEN NULL

    WHEN LOWER(TRIM(clean_currency_hint)) = 'gbp' THEN ROUND(clean_price_local * 1.28, 2)
    WHEN LOWER(TRIM(clean_currency_hint)) = 'eur' THEN ROUND(clean_price_local * 1.10, 2)
    WHEN LOWER(TRIM(clean_currency_hint)) = 'inr' THEN ROUND(clean_price_local * 0.012, 2)
    WHEN LOWER(TRIM(clean_currency_hint)) = 'aud' THEN ROUND(clean_price_local * 0.66, 2)
    WHEN LOWER(TRIM(clean_currency_hint)) = 'jpy' THEN ROUND(clean_price_local * 0.0067, 2)
    WHEN LOWER(TRIM(clean_currency_hint)) = 'usd' THEN ROUND(clean_price_local * 1, 2)
    WHEN LOWER(TRIM(clean_currency_hint)) = 'cad' THEN ROUND(clean_price_local * 0.73, 2)
    WHEN LOWER(TRIM(clean_currency_hint)) = 'brl' THEN ROUND(clean_price_local * 0.20, 2)

    ELSE NULL
END;

-- Finalize cleaned purchases table
RENAME TABLE purchases_raw_copy TO purchases_clean;
