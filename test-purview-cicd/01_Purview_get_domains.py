import os
import json
import requests
from dotenv import load_dotenv

# Azure AD and Purview API configuration (replace with actual values)
load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Purview API endpoints
PURVIEW_ENDPOINT = "https://api.purview-service.microsoft.com"
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

def get_access_token():
    """Obtain an OAuth2 token for the Purview API using client credentials."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://purview.azure.net/.default"
    }
    response = requests.post(AUTH_URL, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

def fetch_business_domains(token):
    """
    Fetch all business domain objects (domains and data products) from Purview.
    Returns a list of dictionary objects.
    """
    url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains?api-version=2025-09-15-preview"
    headers = {"Authorization": f"Bearer {token}"}
    items = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        items.extend(data.get("value", []))
        # Handle pagination if present
        next_token = data.get("skipToken") or data.get("nextLink")
        if next_token:
            url = f"{PURVIEW_ENDPOINT}/datagovernance/catalog/businessdomains?api-version=2025-09-15-preview&$skipToken={next_token}"
        else:
            url = None
    return items

def save_json(file_path, content):
    """
    Save the content dictionary as JSON to file_path, removing any runtime or environment-specific fields.
    """
    # Define fields to remove (runtime info, environment-specific details)
    runtime_fields = [
        "id", "parentId", "systemData", "createdAt", "createdBy",
        "lastModifiedAt", "lastModifiedBy", "expiredAt", "expiredBy",
        "dataQualityScore", "score", "activeSubscriberCount"
    ]
    # Remove runtime fields from top-level
    for field in runtime_fields:
        content.pop(field, None)
    # Remove nested environment-specific data (e.g., related collections in domain references)
    if "domains" in content:
        for domain_ref in content["domains"]:
            domain_ref.pop("relatedCollections", None)
        # Optionally, remove the 'domains' list entirely to avoid storing duplicate or environment-specific info
        content.pop("domains", None)
    # Ensure output directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # Write JSON to file in a readable format
    with open(file_path, "w") as f:
        json.dump(content, f, indent=4)

def main():
    token = get_access_token()
    business_items = fetch_business_domains(token)
    # Define the set of valid domain types (business domains categories in Purview)
    domain_objs = [item for item in business_items]

    # Create a lookup for domain objects by their ID (to help build hierarchical names)
    domain_lookup = {obj["id"]: obj for obj in domain_objs}
    # Function to build full hierarchical domain name (e.g., "Finance.Accounts" for subdomain "Accounts" under "Finance")
    def get_full_domain_name(domain_obj):
        names = ["[" + domain_obj["name"] + "]"]
        parent_id = domain_obj.get("parentId")
        while parent_id:
            parent = domain_lookup.get(parent_id)
            if not parent:
                break
            names.append("[" + parent["name"] + "]")
            parent_id = parent.get("parentId")
        names.reverse()
        return ".".join(names)
    # Write domain JSON files (including subdomains)
    for dom in domain_objs:
        full_name = get_full_domain_name(dom)
        file_path = os.path.join("purview", "unified-catalog", "domains", f"{full_name}.json")
        save_json(file_path, dom.copy())
    print("Metadata export complete.")

if __name__ == "__main__":
    main()