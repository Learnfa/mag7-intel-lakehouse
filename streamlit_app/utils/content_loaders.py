# utils/content_loader.py

from pathlib import Path
import streamlit as st

def load_markdown(path: str, *, expanded: bool = False, title: str | None = None):
    """
    Load and render a markdown file inside an expander.

    Args:
        path: Relative path to markdown file (e.g. "content/s1_momrev_explainer.md")
        expanded: Whether the expander is open by default
        title: Optional expander title (defaults to filename-based)
    """
    p = Path(path)

    expander_title = title or f"About: {p.stem.replace('_', ' ').title()}"

    with st.expander(expander_title, expanded=expanded):
        if not p.exists():
            st.info(f"Markdown file not found: `{p.as_posix()}`")
            return

        try:
            st.markdown(p.read_text(encoding="utf-8"))
        except Exception as e:
            st.error(f"Failed to load markdown: {e}")
