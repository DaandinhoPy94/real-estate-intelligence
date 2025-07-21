# scrapers/cbs/cbs_api_explorer.py - FIXED IMPORTS
"""
CBS API Explorer - Discover working endpoints and data structure
Let's properly investigate the CBS API to find real estate data
"""
import asyncio
import aiohttp
import json
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List  # FIX: Added missing imports

logger = logging.getLogger(__name__)

class CBSAPIExplorer:
    """
    Systematically explore CBS Open Data API to find working endpoints
    """
    
    def __init__(self):
        self.base_urls = [
            "https://opendata.cbs.nl/ODataApi/odata",
            "https://opendata.cbs.nl/ODataApi/OData",
            "https://opendata.cbs.nl/odata",
            "https://odata.cbs.nl/ODataApi/odata"
        ]
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def test_base_urls(self) -> Dict[str, bool]:
        """Test which base URLs are working"""
        results = {}
        
        print("🔌 Testing CBS API Base URLs...")
        
        for base_url in self.base_urls:
            try:
                async with self.session.get(base_url) as response:
                    results[base_url] = response.status == 200
                    if response.status == 200:
                        print(f"✅ Working: {base_url}")
                    else:
                        print(f"❌ Failed: {base_url} (Status: {response.status})")
            except Exception as e:
                results[base_url] = False
                print(f"❌ Error: {base_url} - {e}")
        
        return results
    
    async def discover_datasets(self, base_url: str) -> List[Dict]:
        """Discover available datasets from a working base URL"""
        try:
            print(f"\n📊 Discovering datasets at: {base_url}")
            
            async with self.session.get(base_url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # CBS API returns datasets in 'value' array
                    datasets = data.get('value', [])
                    
                    print(f"📋 Found {len(datasets)} total datasets")
                    
                    # Look for real estate related datasets
                    real_estate_keywords = [
                        'huis', 'woning', 'house', 'price', 'verkoop', 
                        'vastgoed', 'real estate', 'koop', 'woningmarkt',
                        'prijzen', 'transacties', 'bouw'
                    ]
                    
                    relevant_datasets = []
                    for dataset in datasets:
                        name = dataset.get('name', '').lower()
                        title = dataset.get('title', '').lower()
                        description = dataset.get('description', '').lower()
                        
                        # Check if any real estate keywords are present
                        text_to_search = f"{name} {title} {description}"
                        if any(keyword in text_to_search for keyword in real_estate_keywords):
                            relevant_datasets.append({
                                'id': dataset.get('name'),
                                'title': dataset.get('title'),
                                'description': dataset.get('description', ''),
                                'updated': dataset.get('Modified', ''),
                                'url': dataset.get('url', '')
                            })
                    
                    print(f"🏠 Found {len(relevant_datasets)} real estate related datasets")
                    
                    # Show top 5 datasets
                    for i, dataset in enumerate(relevant_datasets[:5], 1):
                        print(f"  {i}. {dataset['id']}: {dataset['title']}")
                    
                    return relevant_datasets
                else:
                    print(f"❌ Failed to get datasets: {response.status}")
                    return []
        except Exception as e:
            print(f"❌ Error discovering datasets: {e}")
            return []
    
    async def explore_dataset_structure(self, base_url: str, dataset_id: str) -> Dict:
        """Explore the structure of a specific dataset"""
        dataset_url = f"{base_url}/{dataset_id}"
        
        print(f"\n🔍 Exploring dataset: {dataset_id}")
        
        try:
            # Get dataset metadata
            async with self.session.get(dataset_url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    structure = {
                        'dataset_id': dataset_id,
                        'base_url': dataset_url,
                        'available_endpoints': [],
                        'table_info': data.get('value', [])
                    }
                    
                    # Test common endpoints
                    endpoints_to_test = [
                        'TypedDataSet',
                        'DataProperties', 
                        'CategoryGroups',
                        'UntypedDataSet',
                        'Dimensions'
                    ]
                    
                    print(f"  🧪 Testing endpoints for {dataset_id}...")
                    
                    for endpoint in endpoints_to_test:
                        endpoint_url = f"{dataset_url}/{endpoint}"
                        try:
                            async with self.session.get(endpoint_url) as ep_response:
                                if ep_response.status == 200:
                                    structure['available_endpoints'].append(endpoint)
                                    print(f"    ✅ {endpoint} - Available")
                                else:
                                    print(f"    ❌ {endpoint} - Status {ep_response.status}")
                        except Exception as e:
                            print(f"    ❌ {endpoint} - Error: {e}")
                    
                    return structure
                else:
                    print(f"❌ Dataset {dataset_id} not accessible: {response.status}")
                    return {}
        except Exception as e:
            print(f"❌ Error exploring dataset {dataset_id}: {e}")
            return {}
    
    async def sample_dataset_data(self, base_url: str, dataset_id: str, endpoint: str = 'TypedDataSet') -> pd.DataFrame:
        """Get sample data from a dataset"""
        url = f"{base_url}/{dataset_id}/{endpoint}"
        
        params = {
            '$format': 'json',
            '$top': 5  # Just get 5 records for sampling
        }
        
        print(f"  📋 Sampling data from {endpoint}...")
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    records = data.get('value', [])
                    
                    if records:
                        df = pd.DataFrame(records)
                        print(f"    📊 Found {len(df)} sample records")
                        print(f"    📝 Columns: {list(df.columns)[:10]}...")  # Show first 10 columns
                        
                        # Show sample record (first few fields)
                        if len(records) > 0:
                            sample_record = records[0]
                            sample_fields = dict(list(sample_record.items())[:5])  # First 5 fields
                            print(f"    🔍 Sample record: {sample_fields}")
                        
                        return df
                    else:
                        print(f"    ⚠️  No data returned from {dataset_id}")
                        return pd.DataFrame()
                else:
                    print(f"    ❌ Failed to get sample data: {response.status}")
                    return pd.DataFrame()
        except Exception as e:
            print(f"    ❌ Error sampling dataset: {e}")
            return pd.DataFrame()
    
    async def comprehensive_cbs_exploration(self) -> Dict:
        """Complete exploration of CBS API"""
        print("🔍 CBS API Comprehensive Exploration")
        print("="*60)
        
        exploration_results = {
            'working_base_urls': [],
            'real_estate_datasets': [],
            'dataset_structures': {},
            'sample_data': {}
        }
        
        # Step 1: Test base URLs
        print("\n1️⃣ Testing Base URLs...")
        base_url_results = await self.test_base_urls()
        
        working_urls = [url for url, works in base_url_results.items() if works]
        exploration_results['working_base_urls'] = working_urls
        
        if not working_urls:
            print("\n❌ No working CBS base URLs found!")
            print("💡 This might be temporary - CBS servers could be down")
            return exploration_results
        
        # Use the first working URL
        working_base_url = working_urls[0]
        print(f"\n✅ Using base URL: {working_base_url}")
        
        # Step 2: Discover datasets
        print("\n2️⃣ Discovering Real Estate Datasets...")
        datasets = await self.discover_datasets(working_base_url)
        exploration_results['real_estate_datasets'] = datasets
        
        if not datasets:
            print("\n⚠️  No real estate datasets found")
            print("💡 CBS might have changed their data categorization")
            return exploration_results
        
        # Step 3: Explore top 3 datasets in detail
        print("\n3️⃣ Exploring Dataset Structures...")
        for i, dataset in enumerate(datasets[:3]):  # Limit to top 3
            dataset_id = dataset['id']
            print(f"\n📊 Dataset {i+1}: {dataset_id}")
            print(f"   📋 Title: {dataset['title']}")
            
            structure = await self.explore_dataset_structure(working_base_url, dataset_id)
            exploration_results['dataset_structures'][dataset_id] = structure
            
            # Step 4: Get sample data
            if structure.get('available_endpoints'):
                endpoint = structure['available_endpoints'][0]
                print(f"\n4️⃣ Sampling data from {dataset_id}/{endpoint}...")
                sample_df = await self.sample_dataset_data(working_base_url, dataset_id, endpoint)
                
                if not sample_df.empty:
                    exploration_results['sample_data'][dataset_id] = {
                        'columns': list(sample_df.columns),
                        'sample_record': sample_df.iloc[0].to_dict() if len(sample_df) > 0 else {},
                        'row_count': len(sample_df)
                    }
        
        return exploration_results
    
    def generate_working_endpoints(self, exploration_results: Dict) -> List[str]:
        """Generate list of working API endpoints for real estate data"""
        working_endpoints = []
        
        base_urls = exploration_results.get('working_base_urls', [])
        datasets = exploration_results.get('real_estate_datasets', [])
        structures = exploration_results.get('dataset_structures', {})
        
        for base_url in base_urls:
            for dataset in datasets:
                dataset_id = dataset['id']
                structure = structures.get(dataset_id, {})
                
                for endpoint in structure.get('available_endpoints', []):
                    full_url = f"{base_url}/{dataset_id}/{endpoint}"
                    working_endpoints.append({
                        'url': full_url,
                        'dataset_id': dataset_id,
                        'dataset_title': dataset['title'],
                        'endpoint': endpoint
                    })
        
        return working_endpoints

# Main exploration function
async def explore_cbs_api():
    """Main function to explore CBS API"""
    async with CBSAPIExplorer() as explorer:
        results = await explorer.comprehensive_cbs_exploration()
        
        print("\n" + "="*60)
        print("🎯 CBS API Exploration Results Summary")
        print("="*60)
        
        print(f"✅ Working base URLs: {len(results['working_base_urls'])}")
        for url in results['working_base_urls']:
            print(f"   - {url}")
        
        print(f"\n🏠 Real estate datasets found: {len(results['real_estate_datasets'])}")
        for dataset in results['real_estate_datasets']:
            print(f"   - {dataset['id']}: {dataset['title']}")
        
        print(f"\n📊 Dataset structures explored: {len(results['dataset_structures'])}")
        for dataset_id, structure in results['dataset_structures'].items():
            endpoints = structure.get('available_endpoints', [])
            print(f"   - {dataset_id}: {len(endpoints)} working endpoints")
        
        print(f"\n📋 Sample data retrieved: {len(results['sample_data'])}")
        for dataset_id, sample_info in results['sample_data'].items():
            cols = len(sample_info['columns'])
            print(f"   - {dataset_id}: {cols} columns, {sample_info['row_count']} rows")
        
        # Generate working endpoints
        working_endpoints = explorer.generate_working_endpoints(results)
        
        if working_endpoints:
            print(f"\n🚀 Ready-to-use API endpoints: {len(working_endpoints)}")
            print("\n📋 Top working endpoints:")
            for endpoint_info in working_endpoints[:3]:  # Show first 3
                print(f"   - {endpoint_info['dataset_id']}: {endpoint_info['url']}")
        
        return results, working_endpoints

if __name__ == "__main__":
    print("🚀 Starting CBS API Exploration...")
    print("🔍 This will systematically test CBS endpoints to find real estate data")
    print("-" * 60)
    
    try:
        results, endpoints = asyncio.run(explore_cbs_api())
        
        print(f"\n🎉 Exploration complete!")
        
        if endpoints:
            print(f"💡 Found {len(endpoints)} working CBS API endpoints for real estate data!")
            
            # Save results for later use
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f'cbs_api_exploration_results_{timestamp}.json'
            
            with open(filename, 'w') as f:
                json.dump({
                    'exploration_results': results,
                    'working_endpoints': endpoints,
                    'exploration_date': datetime.now().isoformat()
                }, f, indent=2, default=str)
            
            print(f"📁 Results saved to: {filename}")
            print(f"\n🚀 Ready to update your CBS collectors with working endpoints!")
        else:
            print(f"⚠️  No working endpoints found - CBS API might be down or restructured")
            print(f"💡 Your fallback mock data strategy is perfect for this scenario!")
            
    except Exception as e:
        print(f"\n❌ Exploration failed: {e}")
        print(f"💡 This shows why fallback strategies are essential in production!")