import os
import json
import requests
from dotenv import load_dotenv

# Load environment variables for Azure AD credentials
load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Purview API endpoints and version
PURVIEW_ENDPOINT = "https://api.purview-service.microsoft.com"  # If not using unified preview endpoint, replace with your Purview account URL
API_VERSION = "2025-09-15-preview"
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

def get_access_token():
    """Obtain an OAuth2 token for the Purview API using client credentials."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://purview.azure.net/.default"
    }
    resp = requests.post(AUTH_URL, data=payload)
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_domains(token):
    """Fetch all business domains from Purview and return a list of domain objects."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    domains = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        domains.extend(data.get("value", []))
        # Handle pagination
        url = data.get("nextLink")  # 'nextLink' provides the next page URL if more results exist
    return domains

def fetch_data_products(token):
    """Fetch all data products from Purview and return a list of product objects."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    products = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        products.extend(data.get("value", []))
        # Handle pagination if present (skipToken or nextLink)
        next_token = data.get("skipToken") or data.get("nextLink")
        if next_token:
            # If next_token is a full URL, extract the skipToken parameter; else use it directly
            if str(next_token).startswith("http"):
                token_param = next_token.split("skipToken=")[-1]
                url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts?api-version={API_VERSION}&$skipToken={token_param}"
            else:
                url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts?api-version={API_VERSION}&$skipToken={next_token}"
        else:
            url = None
    return products

def get_full_domain_path(domain_id, domain_lookup):
    """Build the full domain hierarchy path (with bracketed names) given a domain ID."""
    path_segments = []
    current_id = domain_id
    while current_id:
        domain_obj = domain_lookup.get(current_id)
        if not domain_obj:
            break  # domain id not found in lookup (shouldn't happen if data is consistent)
        path_segments.append(f"[{domain_obj['name']}]")
        current_id = domain_obj.get("parentId")  # move to the parent domain
    path_segments.reverse()
    return ".".join(path_segments)  # join segments like [Root].[Sub1].[Sub2]

def save_json(file_path, content):
    """Save the content dictionary as JSON to file_path, removing runtime-specific fields."""
    # Fields to remove: environment-specific or runtime metadata
    runtime_fields = [
        "systemData", "createdAt", "createdBy", "lastModifiedAt", "lastModifiedBy",
        "expiredAt", "expiredBy", "additionalProperties", "id", "domain"
    ]
    for field in runtime_fields:
        content.pop(field, None)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(content, f, indent=4)

def main():
    token = get_access_token()
    # Fetch all domain objects and build a lookup by ID for hierarchy resolution
    domain_list = fetch_domains(token)
    domain_lookup = {domain["id"]: domain for domain in domain_list}
    
    products = fetch_data_products(token)
    for product in products:
        domain_id = product.get("domain")  # GUID of the product's associated domain
        if not domain_id:
            continue  # skip products not associated with any domain
        # Build full domain path (e.g., [RootDomain].[SubDomain].[LeafDomain])
        domain_path = get_full_domain_path(domain_id, domain_lookup)
        product_name = product["name"]
        # Construct the filename with full domain hierarchy and product name
        file_name = f"{domain_path}.[{product_name}].json"
        file_path = os.path.join("purview", "unified-catalog", "data-products", file_name)
        save_json(file_path, product.copy())
    print("Data product export complete with full domain hierarchy in filenames.")

if __name__ == "__main__":
    main()