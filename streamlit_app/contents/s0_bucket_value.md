### Overview
This page shows the **canonical (validated) S0 core signal state** for a selected ticker over time.

It is intended for **signal monitoring and transparency**, not for evaluating performance, forward returns, or trading strategies.

---

### Buckets
- **`regime_bucket_10`**  
  Long-term price percentile vs rolling history  
  *(1 = cheapest, 10 = most expensive)*

- **`zscore_bucket_10`**  
  Short-term standardized price deviation bucket  
  *(1 = oversold / cheap, 10 = overextended)*

---

### Signal States
- **LONG_SETUP**  
  regime ≤ 3 **and** z-score ≤ 3

- **OVEREXTENDED**  
  regime ≥ 8 **and** z-score ≥ 8

- **NEUTRAL**  
  All other bucket combinations

- **MISSING**  
  One or more required inputs are unavailable

---

### Core Score
- Measures **cheapness strength only** *(range: 0–6)*
- **0** = neutral or expensive conditions  
- Higher values = stronger long-setup conviction
- The score does **not** represent performance or returns

---

### Charts on This Page
- **Signal State Timeline**  
  Shows the daily signal state over time and how often transitions occur.

- **LONG_SETUP Persistence**  
  Shows how many consecutive days the signal remains in LONG_SETUP.

- **State Distribution**  
  Summarizes how frequently each signal state occurs historically.

- **Recent History (Inspectable)**  
  Displays recent rows with full inputs and outputs for inspection and debugging.

---

### Notes
- This page is sourced from **`signal_core` only**
- Performance and forward returns are intentionally excluded
- Use **Research** pages for validation and outcome analysis
