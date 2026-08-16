# Retail Lakehouse — PySpark & Delta Lake

A PySpark and Delta Lake retail analytics pipeline implementing a Bronze–Silver–Gold lakehouse architecture with an ML-ready customer churn dataset.

## Architecture

~~~text
Raw CSV Data
     |
     v
+--------------+
|    BRONZE    |
| Raw -> Delta |
+------+-------+
       |
       v
+--------------+
|    SILVER    |
| Clean/Typed  |
| Deduplicated |
+------+-------+
       |
       v
+--------------------------+
|           GOLD           |
| Business + ML-ready data |
+--------------------------+
| customer_sales           |
| product_sales            |
| daily_sales              |
| customer_churn           |
+--------------------------+
~~~

## Project Structure

~~~text
retail-lakehouse/
|
+-- config/
|   +-- config.py
|
+-- data/
|   +-- generate_sample_data.py
|
+-- src/
|   +-- bronze.py
|   +-- silver.py
|   +-- gold.py
|   +-- churn.py
|   +-- spark_session.py
|
+-- tests/
|   +-- test_silver.py
|   +-- test_gold.py
|   +-- test_churn.py
|
+-- .gitignore
+-- requirements.txt
+-- README.md
~~~

## Technology Stack

| Technology | Version | Purpose |
|---|---|---|
| Python | 3.11 | Pipeline and transformation logic |
| PySpark | 3.5.1 | Distributed data processing |
| Delta Lake | 3.2.0 | Lakehouse table storage |
| Apache Spark | 3.5.1 | Processing engine |
| Pytest | 8.2.0 | Automated testing |

## Pipeline Overview

### Bronze

The Bronze layer ingests raw retail CSV datasets into Delta tables.

Source datasets:

- `customers`
- `products`
- `orders`
- `order_items`

Ingestion metadata is added:

- `_ingested_at`
- `_source_file`

### Silver

The Silver layer cleans and standardizes the Bronze datasets.

Transformations include:

- Data type casting
- String trimming
- Category standardization
- Duplicate removal
- Date and numeric normalization

Silver tables:

- `customers`
- `products`
- `orders`
- `order_items`

### Gold

The Gold layer produces business-oriented analytical datasets:

- `customer_sales` — customer-level purchasing metrics
- `product_sales` — product-level sales metrics
- `daily_sales` — daily sales metrics
- `customer_churn` — ML-ready customer features and churn labels

## Customer Churn

The project includes a rule-based churn-labeling pipeline for downstream ML experimentation.

A customer is labeled as churned when:

~~~text
No previous order
        OR
90+ days since last order
        |
        v
churn_label = 1
~~~

Otherwise:

~~~text
churn_label = 0
~~~

A fixed reference date of `2025-01-01` is used so that generated labels remain reproducible.

The churn dataset includes behavioral features such as:

- `total_orders`
- `total_items`
- `total_spend`
- `avg_order_value`
- `days_since_last_order`
- `country`
- `segment`

This project prepares the dataset for ML experimentation; it does not claim to train or evaluate a predictive churn model.

## Testing

The project includes transformation-level tests using Pytest.

Run the complete test suite:

~~~bash
python -m pytest -v
~~~

Current test result:

~~~text
5 passed
~~~

Tests cover:

- Customer deduplication
- Product standardization
- Customer sales aggregation
- Product sales aggregation
- Churn label generation

## Running the Project

### 1. Create the virtual environment

~~~bash
py -3.11 -m venv .venv
~~~

### 2. Activate the environment

~~~bash
source .venv/Scripts/activate
~~~

### 3. Install dependencies

~~~bash
pip install -r requirements.txt
~~~

### 4. Generate sample data

~~~bash
python data/generate_sample_data.py
~~~

### 5. Run the Bronze layer

~~~bash
python -m src.bronze
~~~

### 6. Run the Silver layer

~~~bash
python -m src.silver
~~~

### 7. Run the Gold layer

~~~bash
python -m src.gold
~~~

### 8. Generate the churn dataset

~~~bash
python -m src.churn
~~~

### 9. Run the complete test suite

~~~bash
python -m pytest -v
~~~

## Design Decisions

### Medallion Architecture

The pipeline separates ingestion, cleaning, and business-level transformations into Bronze, Silver, and Gold layers.

This makes individual stages easier to understand, test, debug, and extend.

### Delta Lake

Delta Lake is used as the storage format for the Bronze, Silver, and Gold tables.

### Reproducible Churn Labels

The churn pipeline uses a fixed reference date rather than the current system date. This ensures that rerunning the project produces consistent labels against the same sample dataset.

### Generated Data

Generated Bronze, Silver, and Gold Delta artifacts are excluded from Git because they can be recreated by running the pipeline.

## Future Enhancements

Potential extensions include:

- Incremental Delta ingestion
- Data quality validation
- Slowly Changing Dimensions
- Spark partitioning and optimization
- Airflow orchestration
- GCP deployment
- BigQuery integration
- Churn model training and evaluation
- CI/CD automation

## Author

**Shreetama Basu**

Data Engineer | GCP | Python | SQL | PySpark
