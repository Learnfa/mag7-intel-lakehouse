# 🧠 Market (Mag7) & Sentiment Intelligence Lakehouse

### **Group Project – GCP × BigQuery × Meltano × dbt × Dagger × Streamlit**

This project implements an end-to-end **data lakehouse + ELT pipeline** with:

* Automated **daily ingestion** (stocks, news, macro)
* BigQuery data warehouse modeling (raw → staging → marts)
* Orchestration using **Dagger**
* Analytics dashboard using **Streamlit**
* Phase 2: ML feature store & predictive models

This README explains how to set up the shared cloud environment, use per-developer datasets, run ingestion/dbt/Dagger locally, and follow good team workflow.

---

# 🚀 0. Prerequisites

Ensure the following are installed locally
* Python 3.11+
* Google Cloud SDK (`gcloud`)
* Conda / Miniconda

## 0.1 Git & Github Repo

```
# 1. clone from github repo
git clone <repo>
cd <repo>

# 2. Create your own branch:
git checkout -b <your-branch-name>

# 3. Update guthub - your branch
git add .
git commit -m "My update"
git push -u origin <your-branch-name>  # do it once, git push subsequently
```

---
# 🚀 1. Project Setup

## 1.1 Conda Environment Setup

Follow these steps to set up an isolated environment for the project:

### Step 1 — Create a new Conda environment

```bash
mkdir mag7_intel  # the project root directory
cd mag_intel
conda create -n mag7 python=3.11 -y
```

### Step 2 — Activate the environment

```bash
conda activate mag7
```

### Step 3 — Create/remove environment variables using Conda activate.d/deactivate.d hook

This steps auto load project-scoped environment variables in Conda environment.

Create conda hook folders
```
mkdir -p "$CONDA_PREFIX/etc/conda/activate.d"
mkdir -p "$CONDA_PREFIX/etc/conda/deactivate.d"
```

Create activate hook shell file
```
nano $CONDA_PREFIX/etc/conda/activate.d/env_vars.sh
```

Put this inside, replace the **service-account json file location and GCP project id**:
```
#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS="</absolute/path/to/your/service-account.json>"
export GCP_PROJECT_ID="<your-gcp-project-id>"
export GCP_REGION="US"
export BQ_DATASET_RAW="mag7_intel_raw"
export BQ_DATASET_STAGING="mag7_intel_staging"
export BQ_DATASET_MARTS="mag7_intel_marts"
```

To clean up the environment var after deactivating the environment, create deactivate hook shell file
```
nano $CONDA_PREFIX/etc/conda/deactivate.d/env_vars.sh
```

Put this inside:
```
#!/bin/bash
unset GOOGLE_APPLICATION_CREDENTIALS
unset GCP_PROJECT_ID
unset GCP_REGION
unset BQ_DATASET_RAW
unset BQ_DATASET_STAGING
unset BQ_DATASET_CORE
unset BQ_DATASET_MARTS
```

To load the environment variables, deactivate and reactivate the project environment
```
conda deactivate
conda activate mag7
```

### Step 4 - Install required Python packages

```bash
pip install -r requirements.txt
```
At start, the following packages are installed:
* altair
* dagger-io (Python SDK)
* dbt-bigquery
* dbt-core
* google-cloud-bigquery>=3.12
* google-cloud-storage>=2.14
* google-auth>=2.23
* jupyterlab
* meltano
* pandas
* plotly
* streamlit

These will grow as the project evolves.


**Verify Python version**

```bash
python --version
```

Should output `Python 3.11.x`.


## 1.2 Bootstrap meltano project

This step creates the Meltano project structure within our project and wires it to BigQuery using a service account JSON key.

### Step 1 - meltano init

**NOTE:** If you clone this project repo, meltano/ is already initialized and committed, you can skip this meltano init step and go to the next step.

**At the project root folder:**
```bash
meltano init meltano
```
This will create a meltano/ folder with a meltano.yml file inside.

### Step 2 - Add BigQuery loader

Add a loader to load data into BigQuery

**cd to the meltano project folder:**
```
meltano add target-bigquery
```
You may run 'meltano config target-bigquery set --interactive' to configure the target-bigquery loader. Alternatively you may add the following config section in the target-bigquery loader section created in the meltano.yml file:
```
  loaders:
  - name: target-bigquery
    variant: z3z1ma
    pip_url: git+https://github.com/z3z1ma/target-bigquery.git
    config:
      credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
      dataset: ${BQ_DATASET_RAW}  # default dataset for Meltano
      denormalized: true
      flattening_enabled: true
      flattening_max_depth: 1
      method: batch_job
      project: ${GCP_PROJECT_ID}
```
The config make use of environment variables loaded in the conda environment.

### Step 3 - Quick Sanity Test

```
meltano invoke target-bigquery --version
```
more meltano taps will be created during development.


## 1.3 Bootstrap dbt project

This step creates the dbt project within our project.

### Step 1 - dbt init

**NOTE:** If you clone this project repo, dbt/ is already initialized and committed, you can skip this dbt init step and go to the next step.

**At the project root folder:**
```bash
dbt init dbt
    project name: mag7_intel
    database: [1] bigquery
    authentication: [2] service_account
    keyfile: </absolute/path/to/your/service-account.json>
    GCP project id: <your-gcp-project-id>
    dataset: mag7_staging
    threads (1 or more): 4
    job_execution_timeout_seconds [300]: 
    Desired location option (enter a number): [1] US
```
This will create a dbt/ folder with a dbt_project.yml file inside.


### Step 2 - Configure dbt profile

Create/edit the profiles.yml file **in dbt directory** with the following content.
```
mag7_intel:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: "{{ env_var('BQ_DATASET_STAGING') }}"   # default dataset for dbt
      location: "{{ env_var('GCP_REGION', 'US') }}"
      threads: 4
      priority: interactive
      timeout_seconds: 300
```

### Step 3 - Quick Sanity Check

**in dbt directory**
```
dbt debug
```

## 1.4 Commands for Creating BigQuery Datasets (US region)

This is optional but we can do ahead and create the datasets in BQ:

```
bq --location=US mk --dataset mag7_intel_raw
bq --location=US mk --dataset mag7_intel_staging
bq --location=US mk --dataset mag7_intel_marts
bq --location=US mk --dataset mag7_intel_ml
bq --location=US mk --dataset mag7_intel_pred
```


---
# 📁 2. Project Structure

```
mag7-intel/
│
├── .env.example
├── .env                 # NOT committed
├── .ignore
├── requirements.txt
├── README.md
│
├── src/
│   └── extractors/
│       ├── stocks_extractor.py
│       ├── news_extractor.py
│       └── macro_ingestor.py
│
├── dbt/
├── meltano/
├── orchestration/
│   └── daster_home/
│   └── orchestration/
│   └── orchestration_tests/
│
└── dashboards/
    └── streamlit/
```


---

# 📥 3. Data Extraction Scripts

### 3.1 Stock Extractor

stocks_extractor.py support two modes:
* `--mode backfill` → extract historical data (run once)
* `--mode incremental` → extract new data based on MAX(date) in BigQuery (daily)

```bash
python src/extractors/stock_extractor.py --mode backfill --universe mag7_with_indexes --include-vix
python src/extractors/stocks_extractor.py --mode incremental --universe mag7_with_indexes --include-vix
```

### 3.2 Google News Extractor

news_extractor.py supports the following modes:
* `--window 1d/7d/30d` → extract older news 1d/7d/30d back, note that google limits to 100 records per ticker
* `--tickers AAPL MSFT` → extract new for specific tickers

```bash
python src/extractors/news_extractor.py
python src/extractors/news_extractor.py --window 1d
python src/extractors/news_extractor.py --tickers AAPL MSFT --window 7d
```

---

# 📥 4. Meltano

### 4.1 meltano install (add tap-csv)

**in meltano folder**
```
meltano add tap-csv
# config tap-csv (files)

meltano job add load_csvs

# if encounter err, manually add this in yml
jobs:
  - name: load_csvs
    tasks:
      - tap-csv target-bigquery
```

### 4.2 meltano run job 
```
meltano run load_csvs  # job name defined in yml
```

# 🏗 5. dbt

### 5.1 dbt seeds
Create seed dims for analytical lookup.
1. Add ticker & calendar csv files in seeds folder

2. Add seeds section in dbt_project.yml
```
seeds:
  mag7_intel:
    +schema: mag7_intel_dims
    tickers:
      +alias: dim_ticker
    calendar:
      +alias: dim_calendar
```

2. Execute & create dims
```
dbt seed
```

### 5.2 staging 

1. Create source.yml

2. type cast & dedup for news and stocks, break stocks into mag7, vix and index
  * create stg_news_headlines.sql
  * create stg_stock_prices_all.sql
  * create stg_stock_prices_mag7.sql
  * create stg_stock_prices_vix.sql
  * create stg_stock_prices_index.sql
  * run & test
    ```
    dbt run
    dbt run # or 
    dbt run -s stg_stock_prices_all  # to run individual module
    dbt test
    ```

3. pull in GDELT & Google Trends from pub data

4. ?

# 🤖 6. Dagster Orchestration

### 6.1 Create dagster scaffold
```
dagster project scaffold --name orchestration

```

### 6.2 create assets.py
Creat asset.py with following assets:
1. news extractor
  csv_path = run_news_extractor(tickers, window)
2. stock price extractor
  csv_path = extract_to_csv(mode, universe, include_vix, tickers)
3. Meltano load (csv → BigQuery)
  subprocess.run(
        ["meltano", "run", "load_csvs"],
        cwd, check, capture_output, text  )
4. DBT Transforms (STAGING)
  subprocess.run(
        dbt_cmd, cwd=DBT_DIR, check=False,
        capture_output=True, text=True, )

### 6.3 Configure definitions.py

### 6.4 Run Dagster

### 6.5 Containerize Dagster (optional)
1. add workspace.yaml in project root
2. add dagster-daemon in requirements.txt
3. pip install -r requirements.txt
4. create docker-compose.yml
5. docker-compose build
6. docker-compose up -d

**in orchestration folder**
```bash
dagster dev
```

---
# 🌐 6. Running Streamlit Dashboard

```bash
streamlit run dashboards/streamlit/app.py
```

Streamlit queries:

* `mart.*`
* `core.*`
