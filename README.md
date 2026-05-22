# bde-project
Big Data Engineer 2-2026 
# Steam Market Analysis Pipeline

Automated data engineering pipeline for collecting and analyzing Steam market data across multiple countries.

## Project Goal

This project collects Steam topseller data from different countries
(Germany, Norway, Sweden) and analyzes:

- price differences
- discount behavior
- genre popularity
- topseller overlaps

The focus of the project is building an end-to-end data engineering pipeline.

## Architecture

Steam Store
    ↓
Web Scraper + Steam API
    ↓
AWS S3 (raw JSON)
    ↓
PySpark ETL
    ↓
Partitioned Parquet Files
    ↓
Databricks / PostgreSQL
    ↓
SQL Analysis & Visualization

## Tech Stack

- Python
- BeautifulSoup
- Requests
- PySpark
- AWS S3
- GitHub Actions
- PostgreSQL
- Databricks
- Delta Lake

## Data Sources

### Steam Search
- Topseller rankings
- Country-specific results

### Steam AppDetails API
- prices
- genres
- categories
- publishers
- discounts

## Pipeline

### Scraper
- daily GitHub Actions job
- stores raw JSON snapshots in S3

### ETL
- PySpark transforms raw JSON
- partitioned parquet generation
- data quality checks

### Analytics
- Databricks SQL analysis
- genre analysis
- country comparisons

## Data Quality Checks

The following checks are implemented:

- duplicate detection
- missing values
- country completeness
- missing API details
- partition validation

## Data Model

### Main Table
- tbl_steam_full

### Analytical Tables
- app_genres
- app_categories

## Example Analyses

- Average prices by country
- Most popular genres
- Discount comparison
- Topseller overlap
- Genre combinations

## Cloud Setup

### AWS S3
- raw data storage
- parquet storage

### Databricks
- Delta tables
- SQL analytics
- visualization

## Project Structure

```text
data_lake/
 ├── raw/
 ├── processed/

.github/workflows/
 ├── scraper.yml
 ├── etl.yml

scraper.py
etl.py
quality.py
db_load.py

```markdown
## How to Run

```markdown
## Run Locally

### Install dependencies

```bash
pip install -r requirements.txt

python scraper.py

python etl.py