This page visualizes the **S1 Momentum / Mean-Reversion (MOM/REV) signal**, which classifies each trading day into one of three actionable states:

- **Momentum (MOM)** – price is strong and trending with controlled volatility  
- **Mean Reversion (REV)** – price is depressed relative to its recent range, with volatility under control  
- **Neutral (NEU)** – conditions do not favor either approach

### How the MOM / REV signal is determined

The signal is derived from **three independent dimensions**:

1. **Price position (regime bucket)**  
   The stock’s current price is ranked within its own **200-day trading range** and assigned to a decile:
   - Buckets **8–10** → price near the top of its range (trend-following context)
   - Buckets **1–3** → price near the bottom of its range (mean-reversion context)

2. **Price deviation (z-score)**  
   The **20-day price z-score** measures how stretched the price is relative to its recent average:
   - Negative z-scores indicate oversold conditions
   - Positive z-scores indicate overextended conditions

3. **Volatility filter**  
   A volatility gate ensures signals are only generated when volatility is **not abnormally high**, avoiding unstable regimes.

**Signal rules (simplified):**
- **Momentum (MOM)**  
  Price above its 100-day moving average  
  AND price in regime buckets 8–10  
  AND volatility z-score < +1  

- **Mean Reversion (REV)**  
  Price in regime buckets 1–3  
  AND price z-score ≤ −1  
  AND volatility is not in the top 20% of its historical range  

All other days are classified as **Neutral (NEU)**.

### How to read the charts

- **Chart A** shows the full price context with background shading indicating the active signal state.
- **Chart B** marks the *entry day* of each MOM or REV regime and shows forward returns (5/10/20 days) as evidence.
- **Charts C & D** evaluate whether these signals historically led to favorable forward returns, using both:
  - all days within a signal state, and
  - entry days only (regime starts).

This page focuses on **signal logic and empirical evidence**, not portfolio construction or execution.
