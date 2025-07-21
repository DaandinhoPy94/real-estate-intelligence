-- infrastructure/database/init.sql
-- Initialize Real Estate Intelligence Database

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS ml;

-- Raw property listings table
CREATE TABLE IF NOT EXISTS raw.property_listings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source VARCHAR(50) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    url TEXT,
    
    -- Property details
    address TEXT NOT NULL,
    postal_code VARCHAR(10),
    city VARCHAR(100) NOT NULL,
    province VARCHAR(100),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    
    -- Property characteristics
    property_type VARCHAR(50),
    listing_type VARCHAR(20),
    price DECIMAL(12,2),
    price_currency VARCHAR(3) DEFAULT 'EUR',
    size_m2 INTEGER,
    plot_size_m2 INTEGER,
    rooms INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    build_year INTEGER,
    energy_label VARCHAR(10),
    
    -- Additional features
    has_garden BOOLEAN,
    has_balcony BOOLEAN,
    has_parking BOOLEAN,
    has_elevator BOOLEAN,
    
    -- Dates
    listed_date TIMESTAMP,
    sold_date TIMESTAMP,
    scraped_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Raw data storage
    raw_data JSONB,
    
    -- Constraints
    UNIQUE(source, source_id)
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('raw.property_listings', 'scraped_at', 
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_listings_postal_code ON raw.property_listings(postal_code);
CREATE INDEX IF NOT EXISTS idx_listings_city ON raw.property_listings(city);
CREATE INDEX IF NOT EXISTS idx_listings_price ON raw.property_listings(price);
CREATE INDEX IF NOT EXISTS idx_listings_source ON raw.property_listings(source);
CREATE INDEX IF NOT EXISTS idx_listings_property_type ON raw.property_listings(property_type);
CREATE INDEX IF NOT EXISTS idx_listings_listed_date ON raw.property_listings(listed_date);

-- GiST index for geospatial queries
CREATE INDEX IF NOT EXISTS idx_listings_location 
ON raw.property_listings USING GIST (ST_Point(longitude, latitude));

-- Market metrics table
CREATE TABLE IF NOT EXISTS marts.market_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    postal_code VARCHAR(10) NOT NULL,
    city VARCHAR(100) NOT NULL,
    metric_date DATE NOT NULL,
    
    -- Price metrics
    avg_price_per_m2 DECIMAL(10,2),
    median_price DECIMAL(12,2),
    min_price DECIMAL(12,2),
    max_price DECIMAL(12,2),
    price_trend_30d DECIMAL(5,2), -- Percentage change
    price_trend_90d DECIMAL(5,2),
    
    -- Volume metrics
    new_listings_count INTEGER,
    sold_count INTEGER,
    total_inventory INTEGER,
    avg_days_on_market DECIMAL(5,1),
    
    -- Demand indicators
    price_to_income_ratio DECIMAL(5,2),
    inventory_months DECIMAL(4,2),
    
    calculated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(postal_code, metric_date)
);

-- ML feature store
CREATE TABLE IF NOT EXISTS ml.features (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    property_id UUID REFERENCES raw.property_listings(id),
    
    -- Location features
    postal_code_encoded INTEGER,
    city_encoded INTEGER,
    distance_to_center_km DECIMAL(6,2),
    neighborhood_avg_income INTEGER,
    
    -- Property features
    price_per_m2 DECIMAL(10,2),
    property_age INTEGER,
    size_category VARCHAR(20),
    rooms_per_m2 DECIMAL(6,4),
    
    -- Market features
    local_price_trend_30d DECIMAL(5,2),
    local_inventory_level VARCHAR(20),
    seasonal_factor DECIMAL(4,2),
    
    -- Derived features
    is_luxury BOOLEAN,
    is_new_construction BOOLEAN,
    has_premium_features BOOLEAN,
    
    created_at TIMESTAMP DEFAULT NOW(),
    feature_version VARCHAR(10) DEFAULT '1.0'
);

-- Model predictions table
CREATE TABLE IF NOT EXISTS ml.predictions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    property_id UUID REFERENCES raw.property_listings(id),
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    
    predicted_price DECIMAL(12,2) NOT NULL,
    confidence_lower DECIMAL(12,2),
    confidence_upper DECIMAL(12,2),
    prediction_interval DECIMAL(4,2), -- 0.95 for 95% CI
    
    -- Feature importance (top 10 features)
    feature_importance JSONB,
    
    -- Model metadata
    prediction_date TIMESTAMP DEFAULT NOW(),
    model_metrics JSONB -- R2, MAE, RMSE, etc.
);

-- Data quality metrics
CREATE TABLE IF NOT EXISTS monitoring.data_quality (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(100) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    check_result BOOLEAN NOT NULL,
    metric_value DECIMAL(12,4),
    threshold_value DECIMAL(12,4),
    details JSONB,
    checked_at TIMESTAMP DEFAULT NOW()
);

-- Continuous aggregates for real-time analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS marts.daily_price_stats
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', scraped_at) AS day,
    postal_code,
    city,
    property_type,
    COUNT(*) as listing_count,
    AVG(price) as avg_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
    AVG(price/NULLIF(size_m2, 0)) as avg_price_per_m2,
    AVG(size_m2) as avg_size_m2
FROM raw.property_listings
WHERE price > 0 AND size_m2 > 0
GROUP BY day, postal_code, city, property_type;

-- Refresh policy for continuous aggregate
SELECT add_continuous_aggregate_policy('marts.daily_price_stats',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Weekly market trends view
CREATE MATERIALIZED VIEW IF NOT EXISTS marts.weekly_trends
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 week', scraped_at) AS week,
    postal_code,
    COUNT(*) as new_listings,
    AVG(price) as avg_price,
    LAG(AVG(price)) OVER (PARTITION BY postal_code ORDER BY time_bucket('1 week', scraped_at)) as prev_week_price,
    (AVG(price) / LAG(AVG(price)) OVER (PARTITION BY postal_code ORDER BY time_bucket('1 week', scraped_at)) - 1) * 100 as price_change_pct
FROM raw.property_listings
WHERE price > 0
GROUP BY week, postal_code;

-- Compression policy for old data
SELECT add_compression_policy('raw.property_listings', INTERVAL '30 days');

-- Retention policy (keep data for 2 years)
SELECT add_retention_policy('raw.property_listings', INTERVAL '2 years');

-- Create roles and permissions
CREATE ROLE IF NOT EXISTS airflow_user LOGIN PASSWORD 'airflow_pass';
CREATE ROLE IF NOT EXISTS api_user LOGIN PASSWORD 'api_pass';
CREATE ROLE IF NOT EXISTS readonly_user LOGIN PASSWORD 'readonly_pass';

-- Grant permissions
GRANT USAGE ON SCHEMA raw, staging, marts, ml TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw, staging, marts, ml TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw, staging, marts, ml TO airflow_user;

GRANT USAGE ON SCHEMA marts, ml TO api_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA marts, ml TO api_user;

GRANT USAGE ON SCHEMA marts TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO readonly_user;