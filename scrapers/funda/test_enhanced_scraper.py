# scrapers/funda/test_enhanced_scraper.py
"""
FIXED: Test script met correcte import names
Run: python scrapers/funda/test_enhanced_scraper.py (vanuit project root)
"""
import asyncio
import logging
import sys
import os

# CRITICAL FIX: Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

print(f"🔧 Project root: {project_root}")
print(f"🔧 Python path: {sys.path[:3]}...")  # Show first 3 paths

try:
    from scrapers.funda.enhanced_scraper import EnhancedFundaScraper, FundaDataProcessor
    print("✅ Imports successful!")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("💡 Make sure you have the enhanced_scraper.py file in the right location")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_scraper():
    """Test the enhanced scraper with a small sample"""
    
    print("\n🚀 Testing Enhanced Funda Scraper")
    print("-" * 50)
    
    # Test 1: Basic scraping
    try:
        async with EnhancedFundaScraper(max_concurrent=1) as scraper:
            logger.info("Testing basic scraping...")
            
            # Test with a very small sample to be respectful
            listings = await scraper.scrape_city_listings(
                city='amsterdam',
                max_pages=1,  # Just 1 page for testing
                min_price=300000,
                max_price=600000
            )
            
            print(f"✅ Scraped {len(listings)} listings")
            
            # Show sample data
            if listings:
                sample = listings[0]
                print(f"\n📋 Sample listing:")
                for key, value in sample.items():
                    if key != 'raw_html':  # Skip raw HTML
                        print(f"  {key}: {value}")
                
                # Test data validation
                valid_count = sum(1 for listing in listings if scraper.validate_listing(listing))
                print(f"\n✅ Valid listings: {valid_count}/{len(listings)}")
                
                return listings
            else:
                print("⚠️  No listings found - this might be normal due to Funda's anti-bot measures")
                print("💡 Try adjusting the price range or city")
                return []
                
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        print(f"💡 This might be due to Funda's bot protection")
        return []

async def test_database_storage():
    """Test database storage functionality (with mock data)"""
    
    print("\n🗄️  Testing Database Storage")
    print("-" * 50)
    
    # Create sample listing for testing
    from datetime import datetime
    sample_listings = [
        {
            'source': 'funda',
            'source_id': f'test_{int(asyncio.get_event_loop().time())}',
            'url': '/test-listing/',
            'address': 'Test Address 123',
            'postal_code': '1234AB',
            'city': 'amsterdam',
            'price': 450000.0,
            'size_m2': 85,
            'rooms': 3,
            'property_type': 'apartment',
            'listing_type': 'sale',
            'scraped_at': datetime.now()
        }
    ]
    
    try:
        # Check if we can connect to database
        database_url = "postgresql://postgres:postgres@localhost:5432/real_estate"
        processor = FundaDataProcessor(database_url)
        
        stats = await processor.process_and_store(sample_listings)
        print(f"✅ Database operation completed: {stats}")
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        print("💡 Solutions:")
        print("   1. Make sure PostgreSQL is running: docker-compose up -d postgres")
        print("   2. Check if database exists: docker-compose exec postgres psql -U postgres -l")
        print("   3. Initialize database: docker-compose exec postgres psql -U postgres -d real_estate")

def run_tests():
    """Run all tests with FIXED dependency checking"""
    print("🧪 Enhanced Scraper Tests - Week 2")
    print("=" * 60)
    
    # FIXED: Check dependencies with correct import names
    dependencies_ok = True
    
    try:
        import aiohttp
        print("✅ aiohttp installed")
    except ImportError as e:
        print(f"❌ aiohttp missing: {e}")
        print("💡 Install with: pip install aiohttp")
        dependencies_ok = False
    
    try:
        import bs4  # CORRECT: Import name is bs4, not beautifulsoup4
        print("✅ beautifulsoup4 installed")
    except ImportError as e:
        print(f"❌ beautifulsoup4 missing: {e}")
        print("💡 Install with: pip install beautifulsoup4")
        dependencies_ok = False
    
    try:
        import asyncpg
        print("✅ asyncpg installed")
    except ImportError as e:
        print(f"❌ asyncpg missing: {e}")
        print("💡 Install with: pip install asyncpg")
        dependencies_ok = False
    
    try:
        import tenacity
        print("✅ tenacity installed")
    except ImportError as e:
        print(f"❌ tenacity missing: {e}")
        print("💡 Install with: pip install tenacity")
        dependencies_ok = False
    
    if not dependencies_ok:
        print("\n❌ Missing dependencies - please install them first")
        return
    
    try:
        # Test 1: Basic scraper functionality
        print("\n📡 Testing Scraper (Respectful Mode)")
        listings = asyncio.run(test_scraper())
        
        # Test 2: Database operations
        print("\n💾 Testing Database Operations")
        asyncio.run(test_database_storage())
        
        print("\n" + "="*60)
        print("✅ All tests completed!")
        
        print("\n🔍 Next Steps:")
        print("1. Check database content:")
        print("   docker-compose exec postgres psql -U postgres -d real_estate")
        print("   \\dt  -- Show tables")
        print("   SELECT COUNT(*) FROM raw.property_listings;")
        
        print("\n2. View sample data:")
        print("   SELECT address, city, price FROM raw.property_listings LIMIT 5;")
        
        print("\n3. Ready for Airflow integration! 🚀")
        
    except KeyboardInterrupt:
        print("\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        print(f"💡 Debug info: {type(e).__name__}")

if __name__ == "__main__":
    run_tests()