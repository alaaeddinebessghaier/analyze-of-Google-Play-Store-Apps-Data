# Google Play Store Big Data Analytics Pipeline using PySpark

## Overview

This project is a Big Data analytics pipeline built using PySpark for analyzing Google Play Store applications and user reviews.

The pipeline performs:

- Data ingestion
- Data cleaning
- Feature engineering
- Exploratory Data Analysis (EDA)
- Sentiment analysis preparation
- Correlation analysis
- Data visualization
- PostgreSQL data warehousing

The project aims to uncover insights about:

- App category popularity
- Rating trends
- Pricing strategies
- User sentiment behavior
- Install distribution
- Size optimization strategies

---

# Technologies Used

- Python 3
- PySpark
- Apache Spark
- PostgreSQL
- Pandas
- NumPy
- Matplotlib
- Seaborn
- Plotly

---

# Dataset

Datasets used:

1. Google Play Store Apps Dataset
2. Google Play Store User Reviews Dataset

Files:

- `googleplaystore.csv`
- `googleplaystore_user_reviews.csv`

---

# Project Structure

```text
play_store_project/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── output/
│   ├── charts/
│   └── logs/
│
├── src/
│   ├── main.py
│   ├── data_loader.py
│   ├── cleaning.py
│   ├── analysis.py
│   └── database.py
│
├── requirements.txt
├── README.md
└── docker-compose.yml
```

---

# Features

## Data Loading

- Reads CSV datasets using predefined Spark schemas
- Handles structured data ingestion efficiently

## Data Cleaning

The pipeline cleans and transforms:

### Price Column

- Removes `$`
- Converts to float

### Installs Column

- Removes `+` and `,`
- Converts to numeric values

### Size Column

- Removes `M`
- Converts to float
- Replaces missing values with average size

### Type Column

- Keeps only valid values (`Free`, `Paid`)

### Date Formatting

- Converts `Last Updated` into Spark DateType

### Sentiment Data

- Converts polarity values to float
- Removes invalid rows
- Handles missing sentiment values

---

# Analysis Performed

## Category Market Share

Determines which app categories dominate the market.

## Average Rating by Category

Analyzes rating quality across app categories.

## Size vs Rating Analysis

Studies how application size impacts ratings.

## Free vs Paid Strategy

Compares ratings between free and paid apps.

## Pricing Trends

Analyzes app pricing distributions across categories.

## Install Distribution

Measures popularity based on installs.

## Correlation Analysis

Computes correlations between numeric features.

## Sentiment Analysis

Analyzes positive, neutral, and negative review distributions.

---

# Data Pipeline Flow

```text
CSV Files
   ↓
PySpark Loading
   ↓
Data Cleaning
   ↓
Feature Engineering
   ↓
EDA & Analysis
   ↓
Visualization
   ↓
PostgreSQL Storage
```

---

# Installation

## Clone Repository

```bash
git clone https://github.com/yourusername/play-store-analytics.git

cd play-store-analytics
```

---

# Create Virtual Environment

```bash
python -m venv venv
```

Activate environment:

### Linux/macOS

```bash
source venv/bin/activate
```

### Windows

```bash
venv\Scripts\activate
```

---

# Install Dependencies

```bash
pip install -r requirements.txt
```

---

# Requirements

Example `requirements.txt`

```text
pyspark
pandas
numpy
matplotlib
seaborn
plotly
psycopg2-binary
```

---

# PostgreSQL Configuration

Create database:

```sql
CREATE DATABASE playstore_db;
```

Update database credentials inside:

```python
save_to_postgres()
```

---

# Running the Project

```bash
python main.py
```

---

# Output

The pipeline generates:

- Cleaned datasets
- Statistical analysis
- Visualizations
- PostgreSQL tables

---

# Sample Visualizations

- Correlation heatmap
- Sentiment distribution
- Price distribution
- App size vs rating

---

# Future Improvements

- Real-time streaming with Spark Streaming
- Machine learning recommendation system
- NLP sentiment classification
- Dashboard integration using Power BI or Tableau
- Docker deployment
- Airflow orchestration

---

# Challenges Solved

- Missing values
- Invalid numeric formatting
- Large-scale processing
- Sentiment normalization
- Schema consistency

---

# Learning Outcomes

This project demonstrates understanding of:

- ETL pipelines
- Big Data processing
- Distributed computing
- Data engineering
- Spark DataFrames
- Data visualization
- Database integration

---

# License

MIT License

---

# Author

Your Name

Data Engineering & Big Data Analytics Project
