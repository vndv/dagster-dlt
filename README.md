# Data Pipeline with Dagster, dlt, and dbt using UV Python

## Overview

This monorepo project demonstrates a modern data pipeline using Dagster for orchestration, dlt (data load tool) for data extraction and loading, and dbt (data build tool) for data transformation. The pipeline is built using UV, a fast Python package installer and resolver, to manage dependencies efficiently.

The goal of this project is to provide a scalable and maintainable data pipeline that can be used to extract data from various sources, load it into a data warehouse, and transform it into meaningful insights.

---

## Features

**Dagster**: Orchestrates the entire data pipeline, ensuring tasks are executed in the correct order and handling dependencies.

**dlt**: Simplifies data extraction and loading from various sources (APIs, databases, etc.) into a data warehouse.

**dbt**: Transforms raw data into structured, analytics-ready tables using SQL-based transformations.

**UV**: A fast and efficient Python package manager used to install and manage dependencies.

---

### Project Structure

```bash
.
├── ny_taxi/                  # Main dbt project directory
│   ├── macros/               # Custom macros for the project
│   ├── models/               # dbt models organized by layers (bronze, silver, gold)
│   │   ├── bronze/           # Raw data models (initial ingestion layer)
│   │   ├── silver/           # Cleaned and transformed data models (intermediate layer)
│   │   └── gold/             # Final data models for analytics and reporting
│   ├── seeds/                # Seed data (e.g., static CSV files)
├── queries/                  # Ad-hoc SQL queries for exploration or debugging
└── src/                      # Source code for orchestration (e.g., Dagster, dlt)
    ├── assets/               # Dagster assets (e.g., pipelines, tasks)
    ├── resources/            # Resources for Dagster (e.g., database connections, APIs)
    └── sources/              # Source code for data extraction and loading (e.g., dlt)
```
---

### Prerequisites

Before running the project, ensure you have the following installed:

Python >=3.12
UV: Install UV using the following command:
```bash
pip install uv
```

Docker 

--- 
### Installation

1. Clone the repository:

```bash
git clone git@github.com:vndv/dagster-dlt.git
cd dagster-dlt
```
2. Set up a virtual environment:
```bash
uv venv --python 3.12
uv sync
```
3. Run docker services (Clickhouse, Minio)

```bash
docker-compose up -d
```
MinioUI localhost:9001
user: minio-user
password: minio-password
 - add access_key in minio

Clickhouse: 
user: default
password: clickhouse_password

In Clickhouse create schema and tables for raw data from queries folder

4. Create a .env file in the root directory and add the necessary environment variables 

```bash
mv .env.example .env
```

5. Install dbt packages in folder with dbt project

```bash
dbt deps
```

6. Start dagster UI http://127.0.0.1:3000 or link in terminl

```bash
uv run dagster dev
```

7. In dagster UI first of all run backfill for dlt assets
after that you can materialize dbt models