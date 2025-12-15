"""
BigQuery client factory and query helpers for Streamlit.

Design goals:
- Single place for BigQuery auth
- Works locally, in Docker, and on VM
- Read-only usage (analytics consumer)
- Plays nicely with st.cache_data
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import textwrap
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest, Forbidden, NotFound, GoogleAPICallError

from config.settings import \
    GCP_PROJECT_ID, \
    GOOGLE_APPLICATION_CREDENTIALS, \
    BQ_DATASET_MART
    
# ---------------------------------------------------------------------
# Client Factory
# ---------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    """
    Create and cache a BigQuery client.

    Auth priority:
    1. GOOGLE_APPLICATION_CREDENTIALS (service account JSON)
    2. Application Default Credentials (ADC)
    """

    if GOOGLE_APPLICATION_CREDENTIALS:
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_APPLICATION_CREDENTIALS
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id or GCP_PROJECT_ID,
        )
    else:
        # ADC: works with `gcloud auth application-default login`
        client = bigquery.Client(project=GCP_PROJECT_ID)

    return client


# ---------------------------------------------------------------------
# Query Runner
# ---------------------------------------------------------------------

def run_query(
    sql: str,
    *,
    job_config: Optional[bigquery.QueryJobConfig] = None,
) -> pd.DataFrame:
    """
    Run a SQL query against BigQuery and return a pandas DataFrame.

    Notes:
    - Intended for SELECT queries only
    - No side effects (no CREATE / INSERT)
    """

    client = get_bq_client()

    # helpful when error messages don’t include the full SQL
    sql_preview = textwrap.shorten(
        " ".join(sql.split()), width=700, placeholder=" ...",
    )

    try:
        query_job = client.query(sql, job_config=job_config)

        try:
            result = query_job.result()  # blocks until finished
        except Exception as e:
            # BigQuery job failures often store structured errors on the job
            job_id = getattr(query_job, "job_id", None)
            errors = getattr(query_job, "errors", None)
            state = getattr(query_job, "state", None)

            raise RuntimeError(
                "BigQuery job failed.\n"
                f"job_id={job_id}\n"
                f"state={state}\n"
                f"errors={errors}\n"
                f"sql_preview={sql_preview}\n"
            ) from e

        return result.to_dataframe(create_bqstorage_client=True)

    except (BadRequest, Forbidden, NotFound, GoogleAPICallError) as e:
        # These exceptions usually contain strong hints (line/col, permissions, not found, location, etc.)
        raise RuntimeError(
            "BigQuery query exception.\n"
            f"{type(e).__name__}: {e}\n"
            f"sql_preview={sql_preview}\n"
        ) from e


# ---------------------------------------------------------------------
# Convenience Helpers
# ---------------------------------------------------------------------

def run_table_query(
    table_name: str,
    *,
    limit: Optional[int] = None,
    where: Optional[str] = None,
    order_by: Optional[str] = None,
) -> pd.DataFrame:
    """
    Convenience helper to SELECT from a mart table.

    Example:
        run_table_query(
            "signal_core",
            where="core_signal_state = 'LONG_SETUP'",
            order_by="trade_date DESC",
            limit=100
        )
    """

    sql = f"SELECT * FROM `{BQ_DATASET_MART}.{table_name}`"

    if where:
        sql += f"\nWHERE {where}"

    if order_by:
        sql += f"\nORDER BY {order_by}"

    if limit:
        sql += f"\nLIMIT {limit}"

    return run_query(sql)


# ---------------------------------------------------------------------
# Diagnostics (for debug)
# ---------------------------------------------------------------------

def test_connection() -> bool:
    """
    Lightweight connectivity test.
    """
    try:
        client = get_bq_client()
        list(client.list_datasets(max_results=1))
        return True
    except Exception:
        return False