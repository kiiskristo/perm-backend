"""
Script to find ETA datasets in the DOL API with robust error handling.
"""
import asyncio
import json
import httpx


async def get_datasets():
    """Get datasets directly using httpx."""
    try:
        url = "https://apiprod.dol.gov/v4/datasets"
        print(f"Fetching datasets from: {url}")
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            
            print(f"Response status code: {response.status_code}")
            print(f"Data type: {type(data)}")
            
            # Extract the actual dataset list based on structure
            if isinstance(data, dict) and 'data' in data:
                datasets = data['data']
                print(f"Found {len(datasets)} datasets in data['data']")
            elif isinstance(data, dict) and 'datasets' in data:
                datasets = data['datasets']
                print(f"Found {len(datasets)} datasets in data['datasets']")
            elif isinstance(data, list):
                datasets = data
                print(f"Found {len(datasets)} datasets in list")
            else:
                print("Unable to extract datasets. Raw response:")
                print(json.dumps(data, indent=2)[:500] + "...")
                datasets = []
            
            return datasets
    except Exception as e:
        print(f"Error fetching datasets: {str(e)}")
        return []


async def find_perm_datasets():
    """Find PERM-related datasets."""
    datasets = await get_datasets()
    
    if not datasets:
        print("No datasets found.")
        return
    
    print(f"\nFound {len(datasets)} total datasets.")
    
    # Print first dataset structure
    if datasets:
        print("\nFirst dataset structure:")
        print(json.dumps(datasets[0], indent=2))
    
    # Find ETA datasets
    eta_datasets = []
    for dataset in datasets:
        agency = dataset.get('agency', {})
        agency_abbr = ""
        
        if isinstance(agency, dict):
            agency_abbr = agency.get('abbr', '')
        elif isinstance(agency, str):
            agency_abbr = agency
            
        if agency_abbr.lower() == 'eta':
            eta_datasets.append(dataset)
    
    print(f"\nFound {len(eta_datasets)} ETA datasets:")
    for idx, dataset in enumerate(eta_datasets, 1):
        name = dataset.get('name', 'No name')
        api_url = dataset.get('api_url', 'No endpoint')
        desc = dataset.get('description', 'No description')
        
        print(f"\n{idx}. Name: {name}")
        print(f"   API Endpoint: {api_url}")
        print(f"   Description: {desc[:100]}..." if len(desc) > 100 else f"   Description: {desc}")
    
    # Find PERM-related datasets
    perm_keywords = ['perm', 'labor certification', 'foreign labor', 'immigration']
    perm_datasets = []
    
    for dataset in datasets:
        name = str(dataset.get('name', '')).lower()
        desc = str(dataset.get('description', '')).lower()
        
        for keyword in perm_keywords:
            if keyword in name or keyword in desc:
                perm_datasets.append(dataset)
                break
    
    print(f"\n\nFound {len(perm_datasets)} datasets potentially related to PERM:")
    for idx, dataset in enumerate(perm_datasets, 1):
        name = dataset.get('name', 'No name')
        api_url = dataset.get('api_url', 'No endpoint')
        desc = dataset.get('description', 'No description')
        
        print(f"\n{idx}. Name: {name}")
        print(f"   API Endpoint: {api_url}")
        agency_info = dataset.get('agency', {})
        if isinstance(agency_info, dict):
            agency_name = agency_info.get('name', 'Unknown agency')
            agency_abbr = agency_info.get('abbr', '')
            print(f"   Agency: {agency_name} ({agency_abbr})")
        else:
            print(f"   Agency: {agency_info}")
        print(f"   Description: {desc}")


if __name__ == "__main__":
    asyncio.run(find_perm_datasets())