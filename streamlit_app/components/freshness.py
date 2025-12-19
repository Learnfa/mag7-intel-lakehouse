import streamlit as st
import pandas as pd

def data_freshness_panel(
    *,
    asof_date: pd.Timestamp | str | None,
    sources: list[str],
    location: str = "sidebar",
):
    """
    Render a small "Data Freshness" panel for consistency across pages.

    Parameters
    ----------
    asof_date: timestamp/string/None
    sources: list of tables/marts used by the page
    location: "sidebar" or "main"
    """
    container = st.sidebar if location == "sidebar" else st

    with container:
        st.markdown("### Data Information")
        if asof_date is None or (isinstance(asof_date, float) and pd.isna(asof_date)):
            st.caption("##### As-of: —")
        else:
            try:
                dt = pd.to_datetime(asof_date)
                st.caption(f"As-of: **{dt.strftime('%Y-%m-%d')}**")
            except Exception:
                st.caption(f"As-of: **{asof_date}**")

        st.markdown("**Sources:**")
        sources_text = "".join(f"• `{s}`  \n" for s in sources)
        st.markdown(sources_text)
