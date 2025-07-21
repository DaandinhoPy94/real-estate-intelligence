-- Simplified database initialization
-- Real Estate Intelligence Database

-- Enable TimescaleDB extension (already available in the image)
CREATE EXTENSION IF NOT EXISTS timescaledb;
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
    rooms INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    build_year INTEGER,
    energy_label VARCHAR(10),
    
    -- Additional features
    has_garden BOOLEAN DEFAULT FALSE,
    has_balcony BOOLEAN DEFAULT FALSE,
    has_parking BOOLEAN DEFAULT FALSE,
    
    -- Dates
    listed_date TIMESTAMP,
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

-- Create essential indexes
CREATE INDEX IF NOT EXISTS idx_listings_postal_code ON raw.property_listings(postal_code);
CREATE INDEX IF NOT EXISTS idx_listings_city ON raw.property_listings(city);
CREATE INDEX IF NOT EXISTS idx_listings_price ON raw.property_listings(price);
CREATE INDEX IF NOT EXISTS idx_listings_source ON raw.property_listings(source);

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
    
    -- Volume metrics
    new_listings_count INTEGER,
    sold_count INTEGER,
    total_inventory INTEGER,
    
    calculated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(postal_code, metric_date)
);

-- ML predictions table
CREATE TABLE IF NOT EXISTS ml.predictions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    property_id UUID REFERENCES raw.property_listings(id),
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    
    predicted_price DECIMAL(12,2) NOT NULL,
    confidence_lower DECIMAL(12,2),
    confidence_upper DECIMAL(12,2),
    
    prediction_date TIMESTAMP DEFAULT NOW()
);

-- Insert some sample data for testing
INSERT INTO raw.property_listings (
    source, source_id, address, postal_code, city, property_type, 
    price, size_m2, rooms, listed_date
) VALUES 
('funda', 'sample_1', 'Damrak 1', '1012JS', 'Amsterdam', 'apartment', 450000, 85, 3, NOW()),
('funda', 'sample_2', 'Coolsingel 100', '3012AG', 'Rotterdam', 'apartment', 320000, 75, 2, NOW()),
('funda', 'sample_3', 'Neude 5', '3512AD', 'Utrecht', 'house', 580000, 120, 4, NOW())
ON CONFLICT (source, source_id) DO NOTHING;

-- Verify setup
SELECT 'Database initialized successfully' AS status;