import json
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Union
import httpx
from fastapi import HTTPException

# Use relative imports if running as a module
try:
    from ..config import get_settings
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.config import get_settings

settings = get_settings()


class DOLAPIClient:
    """Client for interacting with the DOL API."""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or settings.DOL_API_KEY
        self.base_url = settings.DOL_API_BASE_URL
        self.agency = settings.DOL_AGENCY
        self.endpoint = settings.DOL_ENDPOINT
        
        if not self.api_key:
            raise ValueError("DOL API key is required")
        
        if not self.agency or not self.endpoint:
            raise ValueError("DOL agency and endpoint are required")
            
        # Remove any full URLs from the endpoint (should just be an identifier)
        if self.endpoint.startswith("http"):
            self.endpoint = self.endpoint.split("/")[-1]
    
    async def _request(self, url: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make an HTTP request to the DOL API."""
        params = params or {}
        
        # Add API key as a URL parameter (not a header) as per the DOL API documentation
        params["X-API-KEY"] = self.api_key
        
        # Print request URL for debugging
        print(f"Requesting: {url}")
        print(f"With params: {params}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                print(f"HTTP Status Error: {e.response.status_code} - {e.response.text}")
                raise HTTPException(
                    status_code=e.response.status_code,
                    detail=f"DOL API error: {e.response.text}"
                )
            except httpx.RequestError as e:
                print(f"Request Error: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Request error: {str(e)}"
                )
    
    def _build_filter_object(self, filters: Dict[str, Any]) -> str:
        """Build a filter object string for DOL API queries."""
        return json.dumps(filters)
    
    def _format_date(self, date_obj: Union[date, datetime, str]) -> str:
        """Format a date for DOL API queries."""
        if isinstance(date_obj, (date, datetime)):
            return date_obj.strftime("%Y-%m-%d")
        return date_obj
    
    async def get_metadata(self, format: str = "json") -> Dict[str, Any]:
        """Get metadata for the dataset."""
        url = f"{self.base_url}/get/{self.agency}/{self.endpoint}/{format}/metadata"
        return await self._request(url)
    
    async def get_cases(
        self,
        limit: int = 100,
        offset: int = 0,
        fields: Optional[List[str]] = None,
        sort_by: Optional[str] = None,
        sort_dir: Optional[str] = "asc",
        filters: Optional[Dict[str, Any]] = None,
        format: str = "json"
    ) -> Dict[str, Any]:
        """Get cases from the DOL API."""
        url = f"{self.base_url}/get/{self.agency}/{self.endpoint}/{format}"
        
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if fields:
            params["fields"] = ",".join(fields)
        
        if sort_by:
            params["sort_by"] = sort_by
            params["sort"] = sort_dir
        
        if filters:
            params["filter_object"] = self._build_filter_object(filters)
        
        return await self._request(url, params)
    
    async def get_case_by_id(self, case_id: str, fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """Get a specific case by ID."""
        filters = {
            "field": "case_identifier",
            "operator": "eq",
            "value": case_id
        }
        
        result = await self.get_cases(limit=1, fields=fields, filters=filters)
        
        if not result or "data" not in result or not result["data"]:
            raise HTTPException(status_code=404, detail="Case not found")
        
        return result["data"][0]
    
    async def get_cases_by_date_range(
        self,
        start_date: Union[date, datetime, str],
        end_date: Union[date, datetime, str],
        fields: Optional[List[str]] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get cases within a date range."""
        start_date_str = self._format_date(start_date)
        end_date_str = self._format_date(end_date)
        
        filters = {
            "and": [
                {
                    "field": "submit_date",
                    "operator": "gte",
                    "value": start_date_str
                },
                {
                    "field": "submit_date",
                    "operator": "lte",
                    "value": end_date_str
                }
            ]
        }
        
        if status:
            filters["and"].append({
                "field": "status",
                "operator": "eq",
                "value": status
            })
        
        result = await self.get_cases(limit=10000, fields=fields, filters=filters)
        
        if not result or "data" not in result:
            return []
        
        return result["data"]
        
    @classmethod
    async def get_available_datasets(cls) -> List[Dict[str, Any]]:
        """Get a list of available datasets from the DOL API."""
        url = "https://apiprod.dol.gov/v4/datasets"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                print(f"Error getting datasets: {str(e)}")
                return []