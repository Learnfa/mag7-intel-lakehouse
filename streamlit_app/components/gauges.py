# components/gauges.py
# Drop-in replacement: clean Fear & Greed dial (half-donut + needle)
# - No legend
# - Better sizing/alignment (works inside Streamlit cards)
# - Optional title + optional regime caption text

import math
import plotly.graph_objects as go


def fear_greed_dial(
    value: float,
    *,
    title: str = "CNN Fear & Greed Index",
    show_title: bool = True,
    height: int = 240,
) -> go.Figure:
    v = max(0.0, min(100.0, float(value)))

    # CNN-ish bands
    bands = [
        ("Extreme Fear", 0, 25,  "#b22222"),
        ("Fear",         25, 45, "#ff7f0e"),
        ("Neutral",      45, 55, "#d9d9d9"),
        ("Greed",        55, 75, "#2ca02c"),
        ("Extreme Greed",75, 100,"#006400"),
    ]

    seg_vals = [hi - lo for _, lo, hi, _ in bands]
    seg_cols = [c for *_, c in bands]

    fig = go.Figure()

    # Half-donut: PIE + crop bottom via yaxis range
    fig.add_trace(
        go.Pie(
            values=seg_vals,
            marker=dict(colors=seg_cols, line=dict(color="white", width=2)),
            hole=0.72,
            textinfo="none",
            hoverinfo="skip",
            sort=False,
            direction="clockwise",
            rotation=180,
            showlegend=False,
        )
    )

    # Needle mapping: 0 -> left (180deg), 100 -> right (0deg)
    angle_deg = 180 - (v / 100.0) * 180
    angle = math.radians(angle_deg)

    # Slightly raise the hub to avoid bottom cropping
    cx, cy = 0.5, 0.52
    needle_len = 0.34
    x2 = cx + needle_len * math.cos(angle)
    y2 = cy + needle_len * math.sin(angle)

    # Needle
    fig.add_trace(
        go.Scatter(
            x=[cx, x2],
            y=[cy, y2],
            mode="lines",
            line=dict(width=3, color="#14213d"),
            hoverinfo="skip",
            showlegend=False,
        )
    )
    # Hub
#    fig.add_trace(
#        go.Scatter(
#            x=[cx],
#            y=[cy],
#            mode="markers",
#            marker=dict(size=1, color="#14213d"),
#            hoverinfo="skip",
#            showlegend=False,
#        )
#    )

    # Label by value
    def _label(x: float) -> str:
        if x < 25:
            return "Extreme Fear"
        if x < 45:
            return "Fear"
        if x < 55:
            return "Neutral"
        if x < 75:
            return "Greed"
        return "Extreme Greed"

    lbl = _label(v)

    # Center number + label (kept clear of needle)
    fig.add_annotation(
        x=0.5,
        y=0.52,
        text=f"<b>{int(round(v))}</b>",
        showarrow=False,
        font=dict(size=44, color="#14213d"),
    )
    fig.add_annotation(
        x=0.5,
        y=0.37,
        text=f"<b>{lbl}</b>",
        showarrow=False,
        font=dict(size=16, color="#666"),
    )

    # End labels (like typical dashboard dial)
    fig.add_annotation(
        x=0.12,
        y=0.20,
        text="Extreme<br>Fear",
        showarrow=False,
        font=dict(size=12, color="#666"),
    )
    fig.add_annotation(
        x=0.88,
        y=0.20,
        text="Extreme<br>Greed",
        showarrow=False,
        font=dict(size=12, color="#666"),
    )

    fig.update_layout(
        height=height,
        margin=dict(l=0, r=0, t=6 if show_title else 0, b=0),
        paper_bgcolor="white",
        plot_bgcolor="white",
        showlegend=False,
        xaxis=dict(visible=False, range=[0, 1]),
        # Crop bottom half cleanly (this is what makes it a semicircle dial)
        yaxis=dict(visible=False, range=[0.10, 1.00]),
    )

    if show_title:
        fig.update_layout(
            title=dict(
                text=title,
                x=0.02,
                xanchor="left",
                y=0.98,
                yanchor="top",
                font=dict(size=14, color="#111"),
            )
        )

    return fig
