### 1. Crucial Interactive Brokers Settings

When fetching daily bars via `ib_async` for regression analysis, use the following parameter combination:

```python
bars = await ib.reqHistoricalDataAsync(
    contract=contract,
    endDateTime="",
    durationStr="2 Y",
    barSizeSetting="1 day",
    whatToShow="ADJUSTED_LAST",  # CRITICAL: Total return (dividends + splits)
    useRTH=True,                  # CRITICAL: Official exchange close (4:00 PM ET)
    formatDate=1,
    keepUpToDate=False,
)

```

#### Why `useRTH=True` is required

* **Matching Factor Timestamps:** Factor providers calculate daily factor returns using official primary exchange closing prices (e.g., 4:00 PM ET for US equities).
* **Eliminating After-Hours Noise:** Setting `useRTH=False` sets the "close" to the last trade of post-market trading (8:00 PM ET). Late earnings announcements or macro headlines between 4:00 PM and 8:00 PM can cause price movements that have zero counter-part in that day's published factor returns.

#### Why `whatToShow="ADJUSTED_LAST"` is required

* **Total Return vs. Price Return:** Standard price series (`whatToShow="TRADES"`) drop on ex-dividend dates. Without adjusting for distributions, dividend payouts appear as large, artificial negative return spikes in $R_{i,t} = \frac{P_t}{P_{t-1}} - 1$, polluting your regression residuals ($\epsilon_t$).
* `ADJUSTED_LAST` provides split- and dividend-adjusted closing prices, yielding total return series compatible with factor models.

### 2. Key Econometric Considerations for Daily Factor Regressions

Daily factor regressions carry specific statistical nuances compared to monthly or weekly regressions:

#### A. Heteroskedasticity and Autocorrelation (HAC)

Daily asset returns exhibit volatility clustering and minor serial correlation. Standard Ordinary Least Squares (OLS) standard errors will underestimate true uncertainty, yielding artificially inflated $t$-statistics.

* **Fix:** Always estimate standard errors using **Newey-West / HAC (Heteroskedasticity and Autocorrelation Consistent)** covariance matrices with a lag parameter (typically 3–5 days for daily data).

#### B. Asynchronous Trading & Non-Synchronous Closes (Dimson Effect)

If you trade international ETFs listed in the US (e.g., `EEM`, `INDA`) or illiquid ETFs, the underlying assets stop trading hours before the US market close.

* Regressing an international ETF's daily returns against US factors (or global factors with different regional cutoffs) produces artificially low contemporaneous betas and high lagged betas.
* **Fix:** Add lagged factor terms ($F_{t-1}$) to the regression model to capture stale pricing effects:

$$R_{i,t} = \alpha_i + \boldsymbol{\beta}_0 \mathbf{F}_t + \boldsymbol{\beta}_1 \mathbf{F}_{t-1} + \epsilon_{i,t}$$

The true factor sensitivity is then the sum of the coefficients: $\boldsymbol{\beta}_{\text{total}} = \boldsymbol{\beta}_0 + \boldsymbol{\beta}_1$.

#### C. Calendar & Holiday Alignment

Different asset classes and international exchanges observe different trading holidays.

* Always **inner-join** ETF return series with factor return series on dates where both contain valid trading data. Never forward-fill missing daily returns with 0% or previous prices, as this artificially dilutes estimated factor loadings.
