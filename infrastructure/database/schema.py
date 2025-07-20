# infrastructure/database/schema.py
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, JSON, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime

Base = declarative_base()

class PropertyListing(Base):
    """
    Raw property listings from various sources
    """
    __tablename__ = 'property_listings'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source = Column(String, nullable=False)  # funda, pararius, etc.
    source_id = Column(String, nullable=False)
    url = Column(String)
    
    # Property details
    address = Column(String, nullable=False)
    postal_code = Column(String, nullable=False)
    city = Column(String, nullable=False)
    province = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    
    # Characteristics
    property_type = Column(String)  # apartment, house, etc.
    listing_type = Column(String)  # sale, rent
    price = Column(Float)
    size_m2 = Column(Float)
    rooms = Column(Integer)
    bedrooms = Column(Integer)
    bathrooms = Column(Integer)
    build_year = Column(Integer)
    energy_label = Column(String)
    
    # Metadata
    listed_date = Column(DateTime)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    raw_data = Column(JSON)  # Store complete scraped data
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_postal_code', 'postal_code'),
        Index('idx_city', 'city'),
        Index('idx_price', 'price'),
        Index('idx_scraped_at', 'scraped_at'),
        Index('idx_source_id', 'source', 'source_id', unique=True),
    )

class MarketMetrics(Base):
    """
    Aggregated market metrics (materialized views)
    """
    __tablename__ = 'market_metrics'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    postal_code = Column(String, nullable=False)
    metric_date = Column(DateTime, nullable=False)
    
    # Price metrics
    avg_price_per_m2 = Column(Float)
    median_price = Column(Float)
    price_trend_30d = Column(Float)  # Percentage change
    price_trend_90d = Column(Float)
    
    # Volume metrics
    new_listings_count = Column(Integer)
    sold_count = Column(Integer)
    avg_days_on_market = Column(Float)
    
    # Demand indicators
    views_per_listing = Column(Float)
    inventory_months = Column(Float)  # Months of inventory
    
    calculated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_metrics_postal_date', 'postal_code', 'metric_date'),
    )

class PricePrediction(Base):
    """
    ML model predictions
    """
    __tablename__ = 'price_predictions'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    property_id = Column(UUID(as_uuid=True))
    model_version = Column(String, nullable=False)
    
    predicted_price = Column(Float, nullable=False)
    confidence_lower = Column(Float)
    confidence_upper = Column(Float)
    feature_importance = Column(JSON)
    
    predicted_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_prediction_property', 'property_id'),
        Index('idx_prediction_date', 'predicted_at'),
    )

# TimescaleDB setup for time-series
timescale_setup = """
-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Convert listings table to hypertable
SELECT create_hypertable('property_listings', 'scraped_at', 
    chunk_time_interval => INTERVAL '1 week');

-- Create continuous aggregates for real-time analytics
CREATE MATERIALIZED VIEW daily_price_stats
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', scraped_at) AS day,
    postal_code,
    property_type,
    AVG(price) as avg_price,
    AVG(price/NULLIF(size_m2, 0)) as avg_price_per_m2,
    COUNT(*) as listing_count
FROM property_listings
GROUP BY day, postal_code, property_type
WITH NO DATA;

-- Refresh policy
SELECT add_continuous_aggregate_policy('daily_price_stats',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
"""