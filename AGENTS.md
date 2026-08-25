This project runs factor-series analyses on ETF data. At a high level, it must:

- fetch ETF data from IBKR and supplementary data from other sources
- follow a medallion architecture via DuckDB schemas
- construct monthly LOCF panels for ETF fundamental metrics
- build weighted factor return series from each fundamental metric
- select a subset of factors as independent variables
- regress ETF returns on factor returns
- calculate efficient-frontier portfolios