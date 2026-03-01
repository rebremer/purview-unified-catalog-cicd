import os
import json
import uuid
import re
import requests
from dotenv import load_dotenv

# Load Azure credentials from environment variables
load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Purview REST API endpoint and version for Quality rules
PURVIEW_ENDPOINT = "https://api.purview-service.microsoft.com"  # use your Purview account endpoint if not using unified API
QUALITY_API_VERSION = "2025-09-01-preview"  # for data quality rules
CATALOG_API_VERSION = "2025-09-15-preview"  # for catalog (domains, products, assets)
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

# Directory containing the exported rule JSON files
RULES_DIR = os.path.join("purview", "data-quality", "rules")

def get_access_token():
    """Obtain an OAuth2 access token for Purview (client credentials flow)."""
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
    """Fetch all business domains (to build hierarchy for domain ID resolution)."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains?api-version={CATALOG_API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    domains = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        domains.extend(data.get("value", []))
        url = data.get("nextLink")
    return domains

def fetch_all_data_products(token):
    """Fetch all data products (to map product names to IDs within domains)."""
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

def fetch_asset_ids_for_product(token, product_id):
    """Fetch all asset GUIDs related to a given data product."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataProducts/{product_id}/relationships?api-version={CATALOG_API_VERSION}&entityType=DATAASSET"
    headers = {"Authorization": f"Bearer {token}"}
    asset_ids = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        for rel in data.get("value", []):
            asset_id = rel.get("entityId")
            if asset_id:
                asset_ids.append(asset_id)
        url = data.get("nextLink")
    return asset_ids

def fetch_existing_rules(token, domain_id, product_id, asset_id):
    """Fetch all existing data quality rules for a specific asset."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/quality/business-domains/{domain_id}/data-products/{product_id}/data-assets/{asset_id}/rules?api-version={QUALITY_API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    rules = []
    while url:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 404:
            break  # no rules found for this asset
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "value" in data:
            rules.extend(data["value"])
            url = data.get("nextLink")
        else:
            if isinstance(data, list):
                rules.extend(data)
            url = None
    return rules

def resolve_domain_id(domain_segments, children_map):
    """Resolve the leaf domain ID given the full domain path segments (using parent-child mapping)."""
    current_parent = None  # start at root (no parent for root domains, so parent is None)
    for segment in domain_segments:
        children = children_map.get(current_parent, [])
        # find child domain with matching name (account for possible "pre-" prefix differences)
        domain_obj = next((d for d in children if d["name"] == segment or d["name"] == f"pre-{segment}"), None)
        if not domain_obj:
            return None
        current_parent = domain_obj["id"]
    return current_parent  # ID of the final leaf domain

def apply_rule_to_asset(token, domain_id, product_id, asset_id, rule_payload):
    """Create or update a data quality rule for a specific asset using the Purview Data Quality API."""
    rule_id = rule_payload.get("id")  # check if payload already has an id
    if not rule_id:
        # if no ID in payload (likely, since export removed it), generate a new one
        rule_id = str(uuid.uuid4())
        rule_payload["id"] = rule_id
    # Build the API endpoint URL for creating/updating the rule
    url = f"{PURVIEW_ENDPOINT}/datagovernance/quality/business-domains/{domain_id}/data-products/{product_id}/data-assets/{asset_id}/rules/{rule_id}?api-version={QUALITY_API_VERSION}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    resp = requests.put(url, headers=headers, json=rule_payload)
    # If rule already exists and body is identical, Purview might return 304 or 409; treat those as non-fatal.
    if resp.status_code in (304, 409):
        return
    resp.raise_for_status()

def main():
    token = get_access_token()
    domains = fetch_all_domains(token)
    products = fetch_all_data_products(token)
    # Build helper maps for domain and product lookups
    children_map = {}
    domain_map = {}
    for dom in domains:
        domain_map[dom["id"]] = dom
        parent_id = dom.get("parentId")
        children_map.setdefault(parent_id, []).append(dom)
    product_map = {(prod["domain"], prod["name"]): prod["id"] for prod in products if prod.get("id") and prod.get("domain")}
    # Iterate over each exported rule file
    for filename in os.listdir(RULES_DIR):
        if not filename.endswith(".json"):
            continue
        base_name = filename[:-5]  # remove '.json'
        # Parse all bracketed segments from the filename
        segments = re.findall(r'\[\s*([^\]]+)\s*\]', base_name)
        if len(segments) < 3:
            print(f"Skipping unrecognized filename format: {filename}")
            continue
        # Last segment is rule name
        rule_name = segments[-1]
        # Second-to-last segment contains asset ID prefix and asset name (e.g., "AB-AssetName")
        asset_label = segments[-2]
        # Third-to-last segment is product name
        product_name = segments[-3]
        # All preceding segments form the full domain path
        domain_segments = segments[:-3]
        # Resolve domain and product IDs
        domain_id = resolve_domain_id(domain_segments, children_map)
        if not domain_id:
            print(f"Domain path {' > '.join(domain_segments)} not found. Skipping file: {filename}")
            continue
        product_id = product_map.get((domain_id, product_name)) or product_map.get((domain_id, f"pre-{product_name}"))
        if not product_id:
            print(f"Data product '{product_name}' not found in domain '{' > '.join(domain_segments)}'. Skipping file: {filename}")
            continue
        # Determine the target asset's ID by matching the asset prefix and name
        asset_prefix, _, asset_name = asset_label.partition('-')
        asset_ids = fetch_asset_ids_for_product(token, product_id)
        # Filter assets by prefix match (case-insensitive)
        candidates = [aid for aid in asset_ids if aid.lower().startswith(asset_prefix.lower())]
        if not candidates:
            print(f"No asset matching prefix '{asset_prefix}' found in product '{product_name}'. Skipping file: {filename}")
            continue
        asset_id = None
        if len(candidates) == 1:
            asset_id = candidates[0]
        else:
            # Multiple candidates share the prefix; fetch names to identify the correct asset by name
            for aid in candidates:
                try:
                    asset_resp = requests.get(
                        f"{PURVIEW_ENDPOINT}/datagovernance/catalog/dataAssets/{aid}?api-version={CATALOG_API_VERSION}",
                        headers={"Authorization": f"Bearer {token}"}
                    )
                    asset_resp.raise_for_status()
                    asset_info = asset_resp.json()
                except requests.HTTPError:
                    continue
                # Compare asset name (or qualifiedName) to find a match
                asset_display_name = asset_info.get("name") or asset_info.get("displayName") or ""
                if asset_display_name == asset_name:
                    asset_id = aid
                    break
        if not asset_id:
            print(f"Asset '{asset_name}' not found in product '{product_name}'. Skipping file: {filename}")
            continue
        # Load rule definition from file
        with open(os.path.join(RULES_DIR, filename), "r") as f:
            rule_data = json.load(f)
        # Ensure required fields are present in the rule payload
        rule_data["name"] = rule_data.get("name", rule_name)
        rule_data["status"] = rule_data.get("status", "Active")  # default to 'Active' if not present
        # Determine if we should update an existing rule or create a new one
        existing_rules = fetch_existing_rules(token, domain_id, product_id, asset_id)
        existing_rule = next((r for r in existing_rules if r.get("name") == rule_data["name"]), None)
        if existing_rule:
            # Use existing rule's GUID
            rule_id = existing_rule.get("id")
        else:
            # Generate a new GUID for the rule
            rule_id = str(uuid.uuid4())
        rule_data["id"] = rule_id
        # Apply (create or update) the rule via Purview Data Quality API
        try:
            apply_rule_to_asset(token, domain_id, product_id, asset_id, rule_data)
            action = "Updated" if existing_rule else "Created"
            print(f"{action} rule '{rule_data['name']}' on asset '{asset_name}' in product '{product_name}' ({' > '.join(domain_segments)})")
        except requests.HTTPError as e:
            print(f"Failed to apply rule '{rule_data.get('name')}' to asset {asset_id}: {e}")

if __name__ == "__main__":
    main()