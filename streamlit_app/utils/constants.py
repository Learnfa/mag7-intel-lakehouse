# -------------------------------------------------------------------
# S0: bucket value signal state color 
# -------------------------------------------------------------------
S0_SIGNAL_COLORS = {
    "LONG_SETUP": "#2ECC71",      # green
    "NEUTRAL": "#BDC3C7",         # grey
    "OVEREXTENDED": "#E74C3C",    # red
}
# -------------------------------------------------------------------
# S1: Momentum / Reversion signal colors
# -------------------------------------------------------------------

S1_SIGNAL_COLORS = {
    "MOM": {
        "hex": "#2ECC71",          # green
        "rgba_bg": "rgba(46, 204, 113, 0.12)",
        "label": "Momentum",
    },
    "REV": {
        "hex": "#3498DB",          # blue
        "rgba_bg": "rgba(52, 152, 219, 0.12)",
        "label": "Mean Reversion",
    },
    "NEU": {
        "hex": "#B0B0B0",          # neutral grey
        "rgba_bg": "rgba(176, 176, 176, 0.10)",
        "label": "Neutral",
    },
    "MISSING": {
        "hex": "#7F8C8D",
        "rgba_bg": "rgba(127, 140, 141, 0.08)",
        "label": "Missing Data",
    },
}
