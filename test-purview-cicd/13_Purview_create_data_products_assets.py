import os
import json
import re
import requests
from dotenv import load_dotenv

# Load Azure AD credentials from environment
load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Purview API base endpoint and API version (modify if using a specific Purview account endpoint)
PURVIEW_ENDPOINT = "https://api.purview-service.microsoft.com"
API_VERSION = "2025-09-15-preview"
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

# Directory containing asset link JSON files (exported earlier)
ASSET_LINK_DIR = os.path.join("purview", "unified-catalog", "data-assets-link")

def get_access_token():
    """Get an OAuth2 token for the Purview API using client credentials."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://purview.azure.net/.default"
    }
    response = requests.post(AUTH_URL, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

def fetch_all_domains(token):
    """Fetch all business domains from Purview (with parent-child relationships)."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    domains = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        domains.extend(data.get("value", []))
        url = data.get("nextLink")  # loop through pages if more results
    return domains

def fetch_all_data_products(token):
    """Fetch all data products from Purview (with their domain associations)."""
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

def resolve_domain_id(domain_segments, children_map):
    """Resolve the leaf domain's ID given the list of domain name segments (from root to leaf)."""
    current_parent = None  # start at root (no parent)
    for segment in domain_segments:
        children = children_map.get(current_parent, [])
        # Domain names in Purview may have 'pre-' prefix if created via earlier steps, so consider that.
        domain_obj = next((d for d in children if d["name"] == segment or d["name"] == f"pre-{segment}"), None)
        if not domain_obj:
            return None  # domain not found
        current_parent = domain_obj["id"]
    return current_parent  # ID of the final leaf domain

def link_asset_to_product(token, product_id, asset_id):
    """Link a data asset to a data product by creating a relationship in Purview."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts/{product_id}/relationships?api-version={API_VERSION}&entityType=DATAASSET"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    # Prepare relationship payload: link the data asset (entityId) to the data product
    payload = {
        "description": "Linked via script",
        "relationshipType": "Related",
        "entityId": asset_id
    }
    response = requests.post(url, headers=headers, json=payload)
    # If already linked or an error occurs, handle accordingly
    if response.status_code in (400, 409):
        print(f"Skipping linking for asset {asset_id}: relationship may already exist (HTTP {response.status_code}).")
        return
    response.raise_for_status()

def main():
    token = get_access_token()
    domains = fetch_all_domains(token)
    # Build a mapping of parentId -> [child domains] for hierarchy resolution
    children_map = {}
    domain_map = {}
    for dom in domains:
        dom_id = dom["id"]
        domain_map[dom_id] = dom
        parent_id = dom.get("parentId")  # None if root domain
        children_map.setdefault(parent_id, []).append(dom)
    # Build a mapping of (domainId, productName) -> productId for quick lookup
    products = fetch_all_data_products(token)
    product_map = {
        (prod["domain"], prod["name"]): prod["id"]
        for prod in products if prod.get("id") and prod.get("domain")
    }

    # Process each asset link file
    for filename in os.listdir(ASSET_LINK_DIR):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(ASSET_LINK_DIR, filename)
        # Parse the filename to get domain path and product name
        base_name = filename[:-5]  # remove .json extension
        segments = re.findall(r'\[\s*([^\]]+)\s*\]', base_name)
        if len(segments) < 2:
            print(f"Skipping unrecognized file name format: {filename}")
            continue
        product_name = segments[-2]
        asset_label = segments[-1]  # e.g., "AB-AssetName" (Asset ID prefix and name)
        domain_segments = segments[:-2]
        # Resolve the leaf domain ID from the domain segments
        domain_id = resolve_domain_id(domain_segments, children_map)
        if not domain_id:
            print(f"Domain path {' > '.join(domain_segments)} not found. Skipping file {filename}.")
            continue
        # Find the data product ID by domain and product name
        product_id = product_map.get((domain_id, product_name))
        if not product_id:
            # Try product name with 'pre-' prefix if present in Purview
            product_id = product_map.get((domain_id, f"pre-{product_name}"))
        if not product_id:
            print(f"Data product '{product_name}' not found under domain path {' > '.join(domain_segments)}. Skipping file {filename}.")
            continue

        # Load asset ID and qualifiedName from the file content
        with open(filepath, "r") as f:
            asset_data = json.load(f)
        asset_id = asset_data.get("id")
        asset_qname = asset_data.get("qualifiedName") or ""  # empty string if not present
        if not asset_id:
            print(f"No asset ID in {filename}. Skipping.")
            continue

        # Create relationship (link asset to data product)
        try:
            link_asset_to_product(token, product_id, asset_id)
            print(f"Linked asset (ID: {asset_id}) to data product '{product_name}' under domain path {' > '.join(domain_segments)}.")
        except requests.HTTPError as err:
            print(f"Error linking asset {asset_id} to product {product_id}: {err}")
    print("Asset-to-data product linking process completed.")

if __name__ == "__main__":
    main()