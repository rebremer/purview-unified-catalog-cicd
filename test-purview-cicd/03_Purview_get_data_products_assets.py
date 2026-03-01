import os
import json
import requests
from dotenv import load_dotenv

# Load environment variables (Tenant ID, Client ID, Client Secret)
load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Purview API endpoint (use account-specific endpoint if not on unified preview)
PURVIEW_ENDPOINT = "https://api.purview-service.microsoft.com"
API_VERSION = "2025-09-15-preview"
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

def get_access_token():
    """Obtain a bearer token for Purview API."""
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://purview.azure.net/.default"
    }
    resp = requests.post(AUTH_URL, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_all_domains(token):
    """Retrieve all business domains (with their parent/child relationships)."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    domains = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        domains.extend(data.get("value", []))
        url = data.get("nextLink")  # if more pages, nextLink contains the next URL
    return domains

def fetch_all_data_products(token):
    """Retrieve all data products (including their domain associations)."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    products = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        products.extend(data.get("value", []))
        url = data.get("nextLink")
    return products

def fetch_asset_ids_for_product(token, product_id):
    """Fetch all related DataAsset IDs for a given data product."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts/{product_id}/relationships?api-version={API_VERSION}&entityType=DataAsset"
    headers = {"Authorization": f"Bearer {token}"}
    asset_ids = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        for rel in data.get("value", []):
            # Each relationship object has an 'entityId' for the related asset
            asset_id = rel.get("entityId")
            if asset_id:
                asset_ids.append(asset_id)
        url = data.get("nextLink")
    return asset_ids

def fetch_asset_metadata(token, asset_id):
    """Fetch detailed metadata for a given data asset by ID."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataAssets/{asset_id}?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def find_qualified_name(data):
    """Recursively find a non-empty 'qualifiedName' in a nested dictionary or list."""
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "qualifiedName" and isinstance(value, str) and value:
                return value
            # Recurse into nested dictionaries/lists
            found = find_qualified_name(value)
            if found:
                return found
    elif isinstance(data, list):
        for item in value:
            found = find_qualified_name(item)
            if found:
                return found
    return None

def get_full_domain_path(domain_id, domain_map):
    """Build the full domain hierarchy path (e.g., [Root].[Subdomain].[Leaf]) for a domain ID."""
    segments = []
    current = domain_map.get(domain_id)
    while current:
        name = current.get("name")
        if not name:
            break
        segments.insert(0, f"[{name}]")
        parent_id = current.get("parentId")
        current = domain_map.get(parent_id)
    return ".".join(segments)

def main():
    token = get_access_token()
    # Prepare domain map for hierarchy lookup
    domain_objects = fetch_all_domains(token)
    domain_map = {domain["id"]: domain for domain in domain_objects}

    data_products = fetch_all_data_products(token)
    for product in data_products:
        product_id = product.get("id")
        product_name = product.get("name", "UnnamedProduct")
        domain_id = product.get("domain")
        if not product_id or not domain_id:
            continue  # skip if missing necessary info
        domain_path = get_full_domain_path(domain_id, domain_map)
        asset_ids = fetch_asset_ids_for_product(token, product_id)
        for asset_id in asset_ids:
            try:
                asset_metadata = fetch_asset_metadata(token, asset_id)
            except requests.HTTPError as e:
                print(f"Warning: Failed to fetch asset {asset_id} - {e}")
                continue
            # Extract asset name and qualified name
            asset_name = asset_metadata.get("name", "UnknownAsset")
            qualified_name = asset_metadata.get("qualifiedName")
            if not qualified_name:
                qualified_name = find_qualified_name(asset_metadata) or ""
            # Prepare minimal asset info
            asset_info = {
                "id": asset_id,
                "qualifiedName": qualified_name
            }
            # Construct file name with full domain path, product name, and asset name
            file_name = f"{domain_path}.[{product_name}].[{asset_id[:2]}-{asset_name}].json"
            file_path = os.path.join("purview", "unified-catalog", "data-assets-link", file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as outfile:
                json.dump(asset_info, outfile, indent=4)
    print("Data asset link export complete with full domain hierarchy and minimal asset info.")

if __name__ == "__main__":
    main()