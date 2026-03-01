import os
import json
import uuid
import requests
from dotenv import load_dotenv

# Load Azure AD credentials from environment (Tenant ID, Client ID, Client Secret)
load_dotenv()
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Purview API endpoints
PURVIEW_ENDPOINT = "https://api.purview-service.microsoft.com"  # Use account-specific endpoint if needed
API_VERSION = "2025-09-15-preview"
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
DOMAIN_DIR = os.path.join("purview", "unified-catalog", "domains")

def get_access_token():
    """Obtain an OAuth2 token for Purview (Unified Catalog scope)."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://purview.azure.net/.default"
    }
    response = requests.post(AUTH_URL, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

def load_domains_from_files():
    """Load all domain JSON files and parse their hierarchy from filenames."""
    domain_entries = []  # will hold dicts with domain data and hierarchy info
    for filename in os.listdir(DOMAIN_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(DOMAIN_DIR, filename)
            with open(filepath, "r") as f:
                domain_data = json.load(f)
            # Derive hierarchy from filename (without .json extension)
            name_chain = filename[:-5]  # remove ".json"
            parts = [p.strip("[]") for p in name_chain.split(".")]
            domain_data["__parts"] = parts  # e.g. ["Finance", "Accounts", "Receivables"]
            domain_entries.append(domain_data)
    # Sort domains by hierarchy depth (root domains first)
    domain_entries.sort(key=lambda d: len(d["__parts"]))
    return domain_entries


def fetch_existing_domains(token):
    """Retrieve all existing business domains from Purview (name->domain mapping)."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}"}
    all_domains = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        all_domains.extend(data.get("value", []))
        url = data.get("nextLink")
    # Map existing domain names to their full objects for quick reference
    return {domain["name"]: domain for domain in all_domains}

def create_domain(domain, token, parent_id=None):
    """Create a new business domain via POST. Returns the new domain's ID."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # Prepare payload for domain creation
    payload = {
        "name": f"pre-{domain['name']}",
        "description": domain.get("description", ""),
        "type": domain.get("type", "FunctionalUnit"),
        "status": domain.get("status", "Draft"),
        "isRestricted": domain.get("isRestricted", False)
    }
    if parent_id:
        payload["parentId"] = parent_id
    # The API requires a unique id in the request; generate a new UUID for the domain
    payload["id"] = str(uuid.uuid4())

    #print(f"Creating domain '{domain['name']}' with parent ID: {parent_id}")
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["id"]

def update_domain(domain, token, domain_id, parent_id=None):
    """Update an existing business domain via PUT. Returns the domain ID (unchanged)."""
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains/{domain_id}?api-version={API_VERSION}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # Prepare payload for domain update. Include fields to be updated.
    payload = {
        "name": f"pre-{domain['name']}",
        "description": domain.get("description", ""),
        "type": domain.get("type", "FunctionalUnit"),
        "status": domain.get("status", "Draft"),
        "isRestricted": domain.get("isRestricted", False)
    }
    if parent_id:
        payload["parentId"] = parent_id
    else:
        # For root domains, ensure parentId is omitted or set to None (as they have no parent)
        payload.pop("parentId", None)
    # Include the domain's existing id in the payload as required by the API
    payload["id"] = domain_id

    #print(f"Updating domain '{domain['name']}' (ID: {domain_id}) with parent ID: {parent_id}")

    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    return domain_id

def apply_domains(domains_data, token):
    """Create or update domains in Purview, preserving hierarchy."""
    existing = fetch_existing_domains(token)  # map of name -> existing domain object
    #print(existing)
    created_id_map = {}  # map of prefixed domain names to their new IDs (for linking children)
    for domain in domains_data:
        # print(f"Processing domain: {domain['name']} with hierarchy parts: {domain['__parts']}")
        parts = domain["__parts"]
        orig_name = domain["name"]  # original domain name from file (without 'pre-')
        prefixed_name = f"pre-{orig_name}"
        # Determine parent domain's prefixed name (if this domain has a parent)
        parent_prefixed = None
        if len(parts) > 1:
            parent_name = parts[-2]  # immediate parent name in the hierarchy
            parent_prefixed = f"pre-{parent_name}"
            print(f"Domain '{orig_name}' has parent '{parent_name}' (prefixed: '{parent_prefixed}')")
        # Resolve parentId from created or existing domains
        parent_id = None
        if parent_prefixed:
            parent_id = created_id_map.get(parent_prefixed) or existing.get(parent_prefixed, {}).get("id")
            print(f"Created ID Map: {created_id_map}")
            print(f"Parent Prefixed: {parent_prefixed}")  
            print(f"Parent ID: {parent_id}")
        # Create or update the domain
        if prefixed_name in existing:
            # Domain exists: update it
            #print("hier" + prefixed_name)
            domain_id = existing[prefixed_name]["id"]
            new_id = update_domain(domain, token, domain_id, parent_id)
        else:
            # Domain does not exist: create it
            new_id = create_domain(domain, token, parent_id)
        # Store the resulting ID in maps for child references
        created_id_map[prefixed_name] = new_id
        # Also update the existing mapping to include this domain (for subsequent lookups)
        existing[prefixed_name] = {"id": new_id, "name": prefixed_name}
        print(existing[prefixed_name])
    print("Domains creation/update completed successfully.")

if __name__ == "__main__":
    token = get_access_token()
    domains_list = load_domains_from_files()
    print(domains_list)
    apply_domains(domains_list, token)