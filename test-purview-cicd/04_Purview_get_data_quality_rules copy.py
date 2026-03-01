import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Purview API endpoints and versions
PURVIEW_ENDPOINT = "https://api.purview-service.microsoft.com"
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
CATALOG_API_VERSION = "2025-09-15-preview"
QUALITY_API_VERSION = "2025-09-01-preview"

def get_access_token():
    """Authenticate with Azure AD and get an access token for Purview APIs."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://purview.azure.net/.default"
    }
    resp = requests.post(AUTH_URL, data=payload)
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_all_domains(token):
    """Fetch all business domains, including nested (child) domains."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains?api-version={CATALOG_API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    domains = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        domains.extend(data.get("value", []))
        url = data.get("nextLink")  # handle pagination
    return domains

def fetch_all_data_products(token):
    """Fetch all data products in Purview."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts?api-version={CATALOG_API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    products = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        products.extend(data.get("value", []))
        url = data.get("nextLink")
    return products

def fetch_product_assets(token, product_id):
    """Fetch all data asset IDs related to a given data product."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts/{product_id}/relationships?api-version={CATALOG_API_VERSION}&entityType=DATAASSET"
    headers = {"Authorization": f"Bearer {token}"}
    asset_ids = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        for rel in data.get("value", []):
            asset_id = rel.get("entityId")  # each relationship includes the asset's GUID
            if asset_id:
                asset_ids.append(asset_id)
        url = data.get("nextLink")  # continue to next page if present
    return asset_ids

def get_full_domain_path(domain_id, domain_lookup):
    """Construct the full domain hierarchy path (Root.Subdomain.Leaf) from a domain ID using parentId relationships."""
    path_segments = []
    current_id = domain_id
    while current_id:
        domain = domain_lookup.get(current_id)
        if not domain:
            break
        path_segments.insert(0, domain["name"])
        current_id = domain.get("parentId")  # move to parent domain (None if at root)
    # Join segments in bracketed format, e.g., [Root].[Sub].[Leaf]
    return ".".join(f"[{name}]" for name in path_segments)

def fetch_rules_for_asset(token, domain_id, product_id, asset_id):
    """Fetch all data quality rules for a given asset within a specific domain and data product."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/quality/business-domains/{domain_id}/data-products/{product_id}/data-assets/{asset_id}/rules?api-version={QUALITY_API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    rules = []
    while url:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 404:
            break  # no rules or asset not found in quality API
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "value" in data:
            rules.extend(data["value"])
            url = data.get("nextLink")
        else:
            # If API returns a direct list (not paginated)
            if isinstance(data, list):
                rules.extend(data)
            url = None
    return rules

def save_rule_json(file_path, rule_obj):
    """Save a rule's metadata to a JSON file, removing runtime-specific fields."""
    for field in ["id", "createdAt", "createdBy", "lastModifiedAt", "lastModifiedBy", "score",
                  "businessDomain", "dataProduct", "dataAsset", "systemData"]:
        rule_obj.pop(field, None)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(rule_obj, f, indent=4)

def main():
    token = get_access_token()
    domains = fetch_all_domains(token)
    products = fetch_all_data_products(token)
    # Create lookup for domain by ID to get names and parentId
    domain_lookup = {dom["id"]: dom for dom in domains}
    # Iterate over each data product
    for prod in products:
        prod_id = prod.get("id")
        domain_id = prod.get("domain")
        if not prod_id or not domain_id:
            continue
        # Build full domain path (e.g., [Root].[Subdomain].[Leaf]) using parentId hierarchy
        domain_path = get_full_domain_path(domain_id, domain_lookup)
        if not domain_path:
            domain_name = domain_lookup.get(domain_id, {}).get("name", "UnknownDomain")
            domain_path = f"[{domain_name}]"  # fallback to leaf name if full path not built
        product_name = prod.get("name", "UnnamedProduct")
        # Fetch all asset GUIDs in this data product
        asset_ids = fetch_product_assets(token, prod_id)
        for asset_id in asset_ids:
            # Optionally, retrieve asset name for filename (using minimal data from Purview)
            asset_name = asset_id  # default to use GUID if name retrieval fails
            try:
                asset_resp = requests.get(
                    f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataAssets/{asset_id}?api-version={CATALOG_API_VERSION}",
                    headers={"Authorization": f"Bearer {token}"}
                )
                asset_resp.raise_for_status()
                asset_data = asset_resp.json()
                # Prefer asset's name if available
                asset_name = asset_data.get("name") or asset_name
            except requests.HTTPError:
                asset_name = asset_name  # keep GUID as name if we can't retrieve a friendly name
            # Fetch all data quality rules for this asset
            rules = fetch_rules_for_asset(token, domain_id, prod_id, asset_id)
            for rule in rules:
                rule_name = rule.get("name", "UnnamedRule")
                # Construct filename with full domain path, product, asset, and rule
                file_name = f"{domain_path}.[{product_name}].[{asset_id[:2]}-{asset_name}].[{rule_name}].json"
                file_path = os.path.join("purview", "data-quality", "rules", file_name)
                save_rule_json(file_path, rule.copy())
    print("Data quality rule export complete with full domain hierarchy in filenames.")

if __name__ == "__main__":
    main()