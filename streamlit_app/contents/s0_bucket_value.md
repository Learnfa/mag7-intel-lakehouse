This page monitors the **canonical S0 core signal state** for a selected ticker over time.

It is designed for **signal transparency, consistency, and operational monitoring** —  
**not** for evaluating performance, forward returns, or strategy outcomes.

---
## What is the S0 Core Signal?

The S0 core signal classifies each trading day into a **discrete market state** by combining:

- a **long-term price regime** (where price sits within its rolling history), and  
- a **short-term price deviation** (how stretched price is relative to recent behavior).

This produces a stable, interpretable signal used as the **foundation** for downstream strategies and research.

---
## Signal Inputs (Buckets)

### `regime_bucket_10`
- Long-term price position percentile vs rolling history  
- Values range from **1 to 10**
  - **1** = cheapest
  - **10** = most expensive

### `zscore_bucket_10`
- Short-term standardized price deviation bucket  
- Values range from **1 to 10**
  - **1** = most oversold / cheap
  - **10** = most overextended

---
## Signal States (Classification Rules)

Each day is classified into one of the following states:

- **LONG_SETUP**  
  Price is cheap on both dimensions  
  *(regime ≤ 3 **and** z-score ≤ 3)*

- **OVEREXTENDED**  
  Price is expensive on both dimensions  
  *(regime ≥ 8 **and** z-score ≥ 8)*

- **NEUTRAL**  
  Mixed or non-extreme conditions  
  *(all other bucket combinations)*

- **MISSING**  
  One or more required inputs are unavailable

---
## Core Score

- Measures **cheapness strength only** *(range: 0–6)*
- **0** = neutral or expensive conditions  
- Higher values indicate **stronger long-setup conviction**
- The score does **not** represent expected returns or performance

A separate `core_reason` field explains why a ticker is not classified as **LONG_SETUP**, or why a signal is missing.

---
## Charts on this Page

### 🧭 Signal State Timeline
Shows the **daily signal state over time** as a categorical timeline.

- Each marker represents one trading day
- Colors indicate the signal state
- Hovering reveals the underlying buckets, z-scores, and core score

**Use this chart to:**
- Observe how frequently the signal changes state
- Identify clustering of EXTENDED or SETUP regimes
- Validate that state transitions align with market intuition

---
### ⏳ LONG_SETUP Persistence
Shows the **number of consecutive days** the ticker remains in **LONG_SETUP**.

- Increases only while the signal stays in LONG_SETUP
- Resets to zero when the state exits LONG_SETUP

**Use this chart to:**
- Distinguish short-lived signals from persistent regimes
- Assess conviction without referencing performance
- Understand whether setups appear as brief events or sustained conditions

---
### 📊 State Distribution
Summarizes how often each signal state has occurred historically.

- Displays total days spent in LONG_SETUP, NEUTRAL, and OVEREXTENDED
- Provides both numeric counts and a visual comparison

**Use this chart to:**
- Understand the structural behavior of a ticker
- Set expectations for how frequently LONG_SETUP conditions occur
- Compare regime characteristics across tickers

---
### 🔎 Recent History (Inspectable)
Shows the most recent signal rows with full inputs and outputs.

**Use this table to:**
- Inspect recent state changes
- Debug bucket assignments and score calculations
- Validate signal correctness at the row level

---
## What This Page Does *Not* Show

This page intentionally excludes:
- strategy performance
- forward returns
- equity curves
- entry / exit timing

Use **Research** pages to validate outcomes and performance characteristics.

---
## Intended Use

Think of this page as the **source of truth** for the S0 signal:

- “What state is the signal in today?”
- “How stable or noisy is it over time?”
- “Does the signal behave consistently across regimes?”

Downstream strategy logic and research should **reference this signal**, not redefine it.
