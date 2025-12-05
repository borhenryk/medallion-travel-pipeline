# Medallion Travel Analytics Pipeline

A production-ready Data Engineering pipeline using the Medallion Architecture pattern, deployed on Databricks with CI/CD via GitHub Actions.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                        │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   🥉 BRONZE     │   🥈 SILVER     │        🥇 GOLD              │
├─────────────────┼─────────────────┼─────────────────────────────┤
│ Raw Ingestion   │ Cleaned Data    │ Aggregated Analytics        │
│                 │                 │                             │
│ • travel_purch  │ • travel_purch  │ • daily_revenue_metrics     │
│ • user_features │ • users         │ • destination_performance   │
│ • destinations  │ • destinations  │ • user_engagement           │
│                 │                 │ • monthly_summary           │
└─────────────────┴─────────────────┴─────────────────────────────┘
```

## 📊 Data Flow

1. **Bronze Layer**: Raw data ingestion from source tables with audit columns
2. **Silver Layer**: Data cleaning, validation, standardization, and derived features
3. **Gold Layer**: Business-ready aggregations for analytics and reporting

## 🚀 Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Databricks CLI installed
- GitHub account with repository access

### Local Development

```bash
# Clone the repository
git clone https://github.com/borhenryk/medallion-travel-pipeline.git
cd medallion-travel-pipeline

# Validate the bundle
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run medallion_pipeline_job -t dev
```

## 📁 Project Structure

```
medallion-travel-pipeline/
├── databricks.yml              # Bundle configuration
├── resources/
│   └── medallion_job.yml       # Job definitions
├── src/medallion_pipeline/
│   ├── 01_bronze_ingestion.py      # Bronze layer notebook
│   ├── 02_silver_transformations.py # Silver layer notebook
│   ├── 03_gold_aggregations.py     # Gold layer notebook
│   └── 04_data_quality.py          # DQ validation notebook
├── tests/
│   └── test_transformations.py # Unit tests
└── .github/workflows/
    └── ci.yml                  # CI/CD pipeline
```

## 🔧 Configuration

### Required GitHub Secrets

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST` | Workspace URL (e.g., `https://xxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Personal Access Token |

## 📝 License

MIT License

## 👤 Author

Henryk Borzymowski
