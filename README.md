The repo contains two sets of scripts that focus to deploy Governance domains, data products, linking data assets and data quality rules.
- Scripts 01–04: These fetch governance metadata from the test domain and save it to disk.
- Scripts 11–14: These read the saved files and deploy the metadata to the production domain.

Key principles behind the scripts are as follows:
- Logical names are used to identify governance artifacts like domains, data products, and data quality rules. This means no technical keys or hard coded IDs are required.
- If an artifact already exists in the target domain, it is updated instead of recreated. This avoids duplication and supports incremental changes.
- For linking data products to data assets, a runtime-generated key is needed. These keys are specific to the Data Map and can differ between environments.
- To avoid that linking data products needs technical that need to manually updated, the qualified name can be used, too. These qualified names are then used to look up the correct asset in the target environment before linking.
