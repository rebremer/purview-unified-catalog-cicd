import os
import json
import uuid
import re
import requests
from dotenv import load_dotenv

# Load Azure credentials (Tenant ID, Client ID, Client Secret)
load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Purview endpoints and API details
PURVIEW_ENDPOINT = "https://api.purview-service.microsoft.com"  # replace with https://<your-purview-account>.purview.azure.com if not using the unified endpoint
API_VERSION = "2025-09-15-preview"
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
DATA_PRODUCT_DIR = os.path.join("purview", "unified-catalog", "data-products")

def get_access_token():
    """Get an OAuth2 access token for Purview (client credentials flow)."""
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
    """Retrieve all business domains from Purview Unified Catalog."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    domains = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        result = resp.json()
        domains.extend(result.get("value", []))
        url = result.get("nextLink")  # nextLink contains the next page URL if available
    return domains

def fetch_existing_products(token):
    """Retrieve all data products and return a map of (domainId, product name) -> product info."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    products_map = {}
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        for p in data.get("value", []):
            domain_id = p.get("domain")
            name = p.get("name")
            if domain_id and name:
                products_map[(domain_id, name)] = {"id": p["id"]}
        url = data.get("nextLink")  # continue if there is a next page
    return products_map

def load_products_from_files():
    """Load data product JSON files and parse full domain hierarchy and product name from filename."""
    entries = []
    for filename in os.listdir(DATA_PRODUCT_DIR):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(DATA_PRODUCT_DIR, filename)
        with open(filepath, "r") as f:
            product_data = json.load(f)
        base_name = filename[:-5]  # drop the .json extension
        # Extract all content between brackets e.g., [Root], [Sub1], [Sub2], [ProductName]
        segments = re.findall(r'\[\s*([^\]]+)\s*\]', base_name)
        if len(segments) < 2:
            # If the filename doesn't match the expected pattern, skip it
            print(f"Skipping unrecognized filename format: {filename}")
            continue
        domain_segments = segments[:-1]   # all except last are domain names
        product_name = segments[-1]       # last segment is the product name
        # Store parsed info in the product data for use in logic
        product_data["__domain_segments"] = domain_segments
        product_data["__product_name"] = product_name
        entries.append(product_data)
    return entries

def resolve_domain_id(domain_segments, children_by_parent):
    """Get the domain ID by traversing the domain hierarchy from root to leaf using provided segments."""
    current_parent_id = None  # None indicates we start from root level
    for segment in domain_segments:
        # Find child under current_parent_id with matching name (match "Name" or "pre-Name")
        candidates = children_by_parent.get(current_parent_id, [])
        domain_obj = next((d for d in candidates if d["name"] == segment or d["name"] == f"pre-{segment}"), None)
        if not domain_obj:
            return None  # the domain segment was not found under the expected parent
        current_parent_id = domain_obj["id"]
    return current_parent_id  # after iterating all segments, this is the leaf domain's ID

def create_data_product(product_data, token, domain_id):
    """Create a new data product under the specified domain."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # Set up the data product payload
    payload = product_data.copy()
    payload["id"] = str(uuid.uuid4())         # generate a new GUID for the data product
    payload["domain"] = domain_id             # assign to the resolved leaf domain
    payload.setdefault("status", "DRAFT")     # ensure status (defaults to DRAFT if not present)
    payload.setdefault("type", "Master")      # ensure type (defaults to Master if not present)
    # Remove any helper attributes and runtime-specific fields from payload
    for field in ["__domain_segments", "__product_name", "systemData", "createdAt", "createdBy", 
                  "lastModifiedAt", "lastModifiedBy", "expiredAt", "expiredBy", "additionalProperties"]:
        payload.pop(field, None)
    # (Note: we do NOT remove 'id' or 'domain' here, as these are needed for the request)
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["id"]

def update_data_product(product_data, token, product_id, domain_id):
    """Update an existing data product (identified by product_id) to match the provided data."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts/{product_id}?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = product_data.copy()
    payload["id"] = product_id              # must include existing ID in the body for update
    payload["domain"] = domain_id           # ensure correct domain association in case it changed
    payload.setdefault("status", payload.get("status", "DRAFT"))
    payload.setdefault("type", payload.get("type", "Master"))
    payload.setdefault("name", payload.get("name", product_data["__product_name"]))
    # Remove helper attributes and runtime-specific fields
    for field in ["__domain_segments", "__product_name", "systemData", "createdAt", "createdBy", 
                  "lastModifiedAt", "lastModifiedBy", "expiredAt", "expiredBy", "additionalProperties"]:
        payload.pop(field, None)
    # (We do not remove 'id', 'domain', or 'name' from the payload on update)
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    return product_id

def main():
    token = get_access_token()
    # Fetch all domain objects and prepare a parent-child mapping for hierarchy traversal
    domain_list = fetch_all_domains(token)
    children_by_parent = {}
    for dom in domain_list:
        parent = dom.get("parentId") or None
        children_by_parent.setdefault(parent, []).append(dom)
    # Fetch existing data products to identify which need updating
    existing_products = fetch_existing_products(token)  # or fetch_existing_data_products(token) in our functions
    data_products = load_products_from_files()

    for prod in data_products:
        domain_hierarchy = prod["__domain_segments"]
        product_name = prod["__product_name"]
        leaf_domain_id = resolve_domain_id(domain_hierarchy, children_by_parent)
        if not leaf_domain_id:
            print(f"Warning: Domain path {' > '.join(domain_hierarchy)} not found. Skipping product '{product_name}'.")
            continue
        # Check if a data product with this name exists in the resolved domain
        existing_entry = existing_products.get((leaf_domain_id, product_name))
        if existing_entry:
            prod_id = existing_entry["id"]
            update_data_product(prod, token, prod_id, leaf_domain_id)
            print(f"Updated data product '{product_name}' in domain '{' > '.join(domain_hierarchy)}' (ID: {prod_id}).")
        else:
            new_id = create_data_product(prod, token, leaf_domain_id)
            print(f"Created data product '{product_name}' in domain '{' > '.join(domain_hierarchy)}' (ID: {new_id}).")

    print("Data product import completed.")

if __name__ == "__main__":
    main()