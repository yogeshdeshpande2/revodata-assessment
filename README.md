## Folder Structure

    Below is the folder structure of this project for quick reference:

    ```
    ├── bundle/
    │   ├── configs/
    │   │   └── env_config.yaml
    │   ├── notebooks/
    │   │   ├── raw_to_bronze.py
    │   │   ├── bronze_to_silver.py
    │   │   └── silver_to_gold.py
    │   ├── resources/
    │   │   ├── clusters/
    │   │   │   └── ingestion_cluster.yml
    │   │   └── jobs/
    │   │       ├── raw_to_bronze.yml
    │   │       ├── bronze_to_silver.yml
    │   │       └── silver_to_gold.yml
    │   └── __init__.py
    │
    ├── data/
    │   ├── airbnb/
    │   │   └── airbnb.csv
    │   ├── geo/
    │   │   ├── amsterdam_areas/
    │   │   │   └── amsterdam_areas.geojson
    │   │   └── post_codes/
    │   │       └── post_codes.geojson
    │   └── rentals/
    │       └── rentals.json
    │
    ├── src/
    │   ├── utils/
    │   │   ├── config_loader.py
    │   │   ├── databricks_logger.py
    │   │   └── utils.py
    │   ├── bronze_silver.py
    │   ├── raw_bronze.py
    │   ├── silver_gold.py
    │   └── deptry.toml
    │
    ├── tests/
    │   ├── test_bronze_to_silver.py
    │   ├── test_raw_to_bronze.py
    │   └── test_silver_to_gold.py
    │
    ├── azure-pipelines.yml
    ├── build.py
    ├── databricks.yml
    ├── deptry.toml
    ├── MAKEFILE
    ├── pyproject.toml
    ├── requirements.txt
    └── README.md
    ```

## Folder and File Descriptions

# bundle/
Main package directory for pipeline orchestration and configuration

# configs/
Environment configuration files (e.g., `env_config.yaml`). Centralizes all application settings, divided by environment (Test, Acceptance, Production). No hard-coding; all parameters referenced from here.
- **notebooks/**: Python scripts representing Databricks notebooks for each data pipeline layer:
    - `raw_to_bronze.py`: Orchestrates transformation from raw data to bronze layer.
    - `bronze_to_silver.py`: Handles transformation from bronze to silver layer.
    - `silver_to_gold.py`: Handles transformation from silver to gold layer.
    - Each notebook script calls the corresponding transformation logic from the `src/` directory.
- **resources/**: Infrastructure-as-code YAML files:
    - **clusters/**: Cluster configuration YAMLs for Databricks compute resources.
    - **jobs/**: Job definition YAMLs for each pipeline stage, specifying which notebook to run and on which cluster.
- `__init__.py`: Marks the bundle directory as a Python package.

# data/
All datasets and geospatial files used in the pipeline:
- **airbnb/**: Main Airbnb dataset in CSV format, used for analytics and processing.
- **geo/**: Geographical data for spatial analysis.
    - **amsterdam_areas/**: GeoJSON files defining Amsterdam area boundaries.
    - **post_codes/**: GeoJSON files with postal code boundaries for geospatial joins and lookups.
- **rentals/**: Rental data in JSON format, used for enrichment or validation.

# src/
Core transformation logic and utility modules:
- **utils/**: Common utility modules:
    - `config_loader.py`: Loads and parses configuration files.
    - `databricks_logger.py`: Custom logging for Databricks jobs.
    - `utils.py`: General helper functions.
- `bronze_silver.py`: Implements transformation logic from bronze to silver data layer.
- `raw_bronze.py`: Implements transformation logic from raw to bronze data layer.
- `silver_gold.py`: Implements transformation logic from silver to gold data layer.
- `deptry.toml`: (If present) Dependency management for the `src/` folder.

# tests/
Unit tests for each transformation script in `src/`. Each major script has a corresponding test file to ensure correctness and reliability of the data processing logic.

# Top-level Files
- `azure-pipelines.yml`: Azure DevOps pipeline configuration for CI/CD automation.
- `build.py`: Script to automate build and deployment tasks.
- `databricks.yml`: Databricks workflow configuration for orchestrating jobs and clusters.
- `deptry.toml`: Dependency management configuration for the project.
- `MAKEFILE`: Makefile for common build and workflow tasks.
- `pyproject.toml`: Python project metadata and build configuration.
- `requirements.txt`: List of Python dependencies required for the project.
- `README.md`: Project documentation and usage instructions.

    ---

## Deployment

This project uses an automated deployment pipeline defined in `azure-pipelines.yml` to manage the build, test, and deployment lifecycle for Databricks assets. The pipeline leverages Databricks Asset Bundles to deploy code and resources to a Databricks workspace.

Pipeline Stages
1. **Continuous Integration (CI):**
        - Executes pre-commit hooks and code quality checks.
        - Runs unit tests to ensure code correctness and reliability before deployment.

2. **Deploy:**
        - Deploys the codebase and infrastructure resources (notebooks, jobs, clusters) to the Databricks workspace using the asset bundle.
        - Ensures that the latest validated code and configurations are available in the target environment.

Currently, the pipeline is configured to deploy to the `develop` environment. However, it is designed to be easily extended for higher environments (such as staging or production) by parameterizing environment-specific settings and resources. This enables dynamic, environment-driven deployments with minimal changes to the pipeline definition.


## Databricks Bundle Deployment Location
The code will be deployed to the configured Databricks workspace at:
/Workspace/.bundle/revodata-assignment

    
## Technology tools

Python & PySpark 
Azure ADLS Gen2
Azure Data Factory
Azure Databricks
Azure DevOps CICD
Azure DevOps/Github
Azure services


## Infrastructure details

Azure Components
- Resource Group: rg-revodata
- Azure Data Factory: adf-revodata
- Azure Storage Account: sarevodata
- Service Principal: spn-revodata
- Access Connector for Azure Databricks: unity-catalog-access-connector

Azure Databricks Components
- Workspace: dbr-revodata
- Compute: All-purpose Personal Compute (revo_cluster)
- Catalog: catalog_revodata
- Schemas: db_revodata_bronze, db_revodata_silver & db_revodata_gold
- Credentials: cred-revodata
- External Locations:
    - ext-loc-revodata-bronze
    - ext-loc-revodata-con-raw
    - ext-loc-revodata-con-revodata
    - ext-loc-revodata-gold
    - ext-loc-revodata-silver


## Orchestration

Azure Data Factory (ADF) serves as the orchestration layer for this solution. The orchestration process is structured as follows:

- **ADF as Orchestrator:** Azure Data Factory coordinates the end-to-end data pipeline, managing dependencies and execution flow.
- **Linked Service:** A linked service is configured in ADF to securely connect to the Azure Databricks workspace.
- **Master Pipeline:** The master pipeline in ADF triggers three sequential Databricks job tasks. Each task launches a Databricks job responsible for transforming data at a specific layer (Raw to Bronze, Bronze to Silver, Silver to Gold), following the medallion architecture.

This approach ensures modular, scalable, and automated orchestration of all data transformation stages within the Azure ecosystem.


## Medallion Architecture

This project implements the medallion architecture, a layered approach to data engineering that improves data quality and enables scalable analytics:

- **Raw Layer:**
    - Stores ingested data in its original format, exactly as received from source systems.
- **Bronze Layer:**
    - Converts raw files into Delta tables for each entity, providing a structured and queryable format while preserving the original data.
- **Silver Layer:**
    - Cleans and enriches the bronze tables, applying data quality rules and transformations to produce refined, analytics-ready tables.
- **Gold Layer:**
    - Aggregates and curates data from the silver layer to create business-focused tables, supporting reporting and advanced analytics use cases.