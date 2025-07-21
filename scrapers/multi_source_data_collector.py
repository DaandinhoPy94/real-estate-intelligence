# scrapers/multi_source_data_collector.py - FIXED VERSION
"""
Multi-Source Real Estate Data Collection Strategy - SYNTAX FIXED
"""
import asyncio
import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import logging
from dataclasses import dataclass
import random

logger = logging.getLogger(__name__)

@dataclass
class PropertyListing:
    """Standardized property data structure across all sources"""
    source: str
    source_id: str
    address: str
    postal_code: str
    city: str
    price: float
    size_m2: int
    rooms: int
    property_type: str
    build_year: Optional[int] = None
    has_garden: Optional[bool] = None
    has_parking: Optional[bool] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    scraped_at: datetime = None
    
    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.now()

class CBSDataCollector:
    """
    CBS (Central Bureau of Statistics) Open Data Integration - FIXED
    """
    
    def __init__(self):
        self.base_url = "https://opendata.cbs.nl/ODataApi/odata/83625NED/v1"
        self.session = None
    
    async def fetch_regional_statistics(self, regions: List[str] = None) -> pd.DataFrame:
        """
        Fetch official regional real estate statistics - FIXED f-string
        """
        if not regions:
            regions = ['Amsterdam', 'Rotterdam', 'Utrecht', 'Den Haag']
        
        # FIX: Create region filter without backslashes in f-string
        region_quotes = [f"'{r}'" for r in regions]
        region_list = ','.join(region_quotes)
        region_filter = f"RegioS in ({region_list})"
        
        # CBS OData query for house price statistics
        query_params = {
            '$format': 'json',
            '$filter': region_filter,
            '$select': 'RegioS,Perioden,GemiddeldeVerkoopprijs_1,AantalVerkopen_2'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url + "/TypedDataSet", params=query_params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._process_cbs_data(data)
                    else:
                        logger.error(f"CBS API error: {response.status}")
                        return pd.DataFrame()
        except Exception as e:
            logger.error(f"CBS data collection failed: {e}")
            return pd.DataFrame()
    
    def _process_cbs_data(self, raw_data: Dict) -> pd.DataFrame:
        """Process CBS JSON response into clean DataFrame"""
        try:
            records = raw_data.get('value', [])
            processed = []
            
            for record in records:
                processed.append({
                    'region': record.get('RegioS'),
                    'period': record.get('Perioden'),
                    'avg_sale_price': record.get('GemiddeldeVerkoopprijs_1'),
                    'transaction_count': record.get('AantalVerkopen_2'),
                    'source': 'cbs_official',
                    'collected_at': datetime.now()
                })
            
            return pd.DataFrame(processed)
        except Exception as e:
            logger.error(f"CBS data processing failed: {e}")
            return pd.DataFrame()

class MockDataGenerator:
    """
    Generate realistic synthetic property data for development - WORKING VERSION
    """
    
    def __init__(self):
        # Dutch cities with realistic price multipliers
        self.city_data = {
            'Amsterdam': {'price_multiplier': 1.4, 'base_price': 400000},
            'Rotterdam': {'price_multiplier': 0.9, 'base_price': 280000},
            'Utrecht': {'price_multiplier': 1.2, 'base_price': 350000},
            'Den Haag': {'price_multiplier': 1.1, 'base_price': 320000},
            'Eindhoven': {'price_multiplier': 0.8, 'base_price': 250000},
            'Groningen': {'price_multiplier': 0.7, 'base_price': 220000}
        }
        
        self.property_types = ['apartment', 'house', 'penthouse', 'studio']
        self.street_names = [
            'Damrak', 'Herengracht', 'Prinsengracht', 'Kalverstraat',
            'Coolsingel', 'Lijnbaan', 'Witte de Withstraat',
            'Neude', 'Oudegracht', 'Nachtegaalstraat'
        ]
    
    def generate_listings(self, count: int = 100) -> List[PropertyListing]:
        """Generate realistic property listings"""
        listings = []
        
        for i in range(count):
            city = random.choice(list(self.city_data.keys()))
            city_info = self.city_data[city]
            
            # Generate realistic property characteristics
            property_type = random.choice(self.property_types)
            
            # Size based on property type
            if property_type == 'studio':
                size_m2 = random.randint(25, 60)
                rooms = 1
            elif property_type == 'apartment':
                size_m2 = random.randint(50, 150)
                rooms = random.randint(2, 4)
            elif property_type == 'penthouse':
                size_m2 = random.randint(100, 300)
                rooms = random.randint(3, 6)
            else:  # house
                size_m2 = random.randint(80, 250)
                rooms = random.randint(3, 6)
            
            # Price calculation with realistic variation
            base_price = city_info['base_price']
            price_per_m2 = (base_price / 100) * city_info['price_multiplier']
            
            # Add randomness and property type adjustments
            type_multiplier = {
                'studio': 0.9,
                'apartment': 1.0,
                'house': 1.1,
                'penthouse': 1.5
            }[property_type]
            
            price = (price_per_m2 * size_m2 * type_multiplier * 
                    random.uniform(0.8, 1.2))  # ±20% variation
            
            # Generate address
            street_name = random.choice(self.street_names)
            house_number = random.randint(1, 500)
            address = f"{street_name} {house_number}"
            
            # Generate postal code (Dutch format: 1234AB)
            postal_digits = random.randint(1000, 9999)
            postal_letters = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
            postal_code = f"{postal_digits}{postal_letters}"
            
            # Optional features (probability based)
            has_garden = random.random() < 0.4  # 40% chance
            has_parking = random.random() < 0.6  # 60% chance
            
            # Build year
            build_year = random.randint(1950, 2023) if random.random() < 0.8 else None
            
            listing = PropertyListing(
                source='mock_generator',
                source_id=f'mock_{i:06d}',
                address=address,
                postal_code=postal_code,
                city=city,
                price=round(price),
                size_m2=size_m2,
                rooms=rooms,
                property_type=property_type,
                build_year=build_year,
                has_garden=has_garden,
                has_parking=has_parking,
                latitude=52.0 + random.uniform(-1, 1),  # Rough Netherlands bounds
                longitude=5.0 + random.uniform(-2, 2)
            )
            
            listings.append(listing)
        
        return listings

class MultiSourceDataCollector:
    """
    Orchestrate data collection from multiple sources - SIMPLIFIED VERSION
    """
    
    def __init__(self):
        self.cbs_collector = CBSDataCollector()
        self.mock_generator = MockDataGenerator()
    
    async def collect_comprehensive_data(self, target_count: int = 100) -> List[PropertyListing]:
        """
        Collect data from multiple sources with fallback strategy
        """
        all_listings = []
        
        # 1. Try CBS for market context (not individual listings)
        try:
            cbs_data = await self.cbs_collector.fetch_regional_statistics()
            logger.info(f"Collected CBS statistics: {len(cbs_data)} records")
            # CBS data would be stored separately for market analysis
        except Exception as e:
            logger.warning(f"CBS collection failed: {e}")
        
        # 2. Generate mock data for individual listings
        mock_listings = self.mock_generator.generate_listings(target_count)
        all_listings.extend(mock_listings)
        logger.info(f"Generated {len(mock_listings)} mock listings")
        
        logger.info(f"Total data collected: {len(all_listings)} listings")
        return all_listings

# Test function
if __name__ == "__main__":
    async def test_collector():
        print("🧪 Testing Multi-Source Data Collector - FIXED VERSION")
        print("=" * 60)
        
        collector = MultiSourceDataCollector()
        
        # Test 1: Mock data generation
        print("🎭 Testing Mock Data Generation...")
        mock_listings = collector.mock_generator.generate_listings(5)
        
        print(f"✅ Generated {len(mock_listings)} realistic listings:")
        for i, listing in enumerate(mock_listings, 1):
            print(f"  {i}. 📍 {listing.address}, {listing.city}")
            print(f"      💰 €{listing.price:,} | {listing.size_m2}m² | {listing.rooms} rooms")
            print(f"      🏠 {listing.property_type} | Built: {listing.build_year}")
            print()
        
        # Test 2: CBS data collection
        print("🏛️ Testing CBS Data Collection...")
        try:
            cbs_collector = CBSDataCollector()
            cbs_data = await cbs_collector.fetch_regional_statistics(['Amsterdam'])
            print(f"📊 CBS data: {len(cbs_data)} records")
            
            if not cbs_data.empty:
                print("Sample CBS data:")
                print(cbs_data.head())
            else:
                print("⚠️  No CBS data returned (API might be down or endpoint changed)")
                
        except Exception as e:
            print(f"❌ CBS test failed: {e}")
        
        # Test 3: Comprehensive collection
        print("\n🔄 Testing Comprehensive Data Collection...")
        try:
            all_data = await collector.collect_comprehensive_data(10)
            print(f"✅ Total collected: {len(all_data)} listings")
            
            # Summary statistics
            cities = [listing.city for listing in all_data]
            city_counts = {city: cities.count(city) for city in set(cities)}
            print(f"📊 Distribution by city: {city_counts}")
            
            avg_price = sum(listing.price for listing in all_data) / len(all_data)
            print(f"💰 Average price: €{avg_price:,.0f}")
            
        except Exception as e:
            print(f"❌ Comprehensive collection failed: {e}")
        
        print("\n🎉 Multi-source data collector ready!")
        print("💡 Next: Integrate with database and Airflow!")
    
    # Run the test
    asyncio.run(test_collector())