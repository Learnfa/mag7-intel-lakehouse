# 🧠 Market (Mag7) & Sentiment Intelligence Lakehouse

### **Group Project – GCP × BigQuery × Meltano × dbt × Dagger × Streamlit**

This project implements an end-to-end **data lakehouse + ELT pipeline** with:

* Automated **daily ingestion** (stocks, news, macro)
* BigQuery data warehouse modeling (staging → core → marts)
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
nano $CONDA_PREFIX/etc/conda/activate.d/env_vars.sh
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
      credentials_path: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      dataset: "{{ env_var('BQ_DATASET_RAW') }}"  # default dataset for Meltano
      denormalized: true
      flattening_enabled: true
      flattening_max_depth: 1
      method: batch_job
      project: "{{ env_var('GCP_PROJECT_ID') }}"
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
bq --location=US mk --dataset mag7_intel_mart
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
├── dagger/
│   └── main.py
│
└── dashboards/
    └── streamlit/
```


---

# 📥 3. Running Data Extraction Scripts

stock extractor.py support two modes:

* `--mode backfill` → extract historical data (run once)
* `--mode incremental` → extract new data based on MAX(date) in BigQuery (daily)

### Example: Stock Prices (yfinance)

```bash
python src/ingestion/stocks_ingestor.py --mode backfill
python src/ingestion/stocks_ingestor.py --mode incremental
```

---

# 🏗 4. Running dbt Transformations


# 🤖 5. Running Dagger Pipeline

```bash
python dagger/main.py
```

Dagger performs Phase 1 ELT:

1. Ingestion (incremental)
2. dbt deps
3. dbt run
4. dbt test

The shared CI pipeline will use:

```
BQ_DATASET_RAW=raw
GCS_LAKE_BUCKET=mag7-intel-lake
```

---

# 🌐 6. Running Streamlit Dashboard

```bash
streamlit run dashboards/streamlit/app.py
```

Streamlit queries:

* `mart.*`
* `core.*`
