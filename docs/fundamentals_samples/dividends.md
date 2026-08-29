## Dividends

Depending on the product, the endpoint can return two different payloads

### Short response

Header: 
https://www.interactivebrokers.ie/tws.proxy/fundamentals/dividends/564156940

Response:
```
{
    "industry_average": {
        "dividend_yield": "1.22%",
        "annual_dividend": "25.65",
        "paying_companies": 0,
        "paying_companies_percent": 0
    },
    "no_dividend_text": "No dividend data available for URNU in the past 5 years",
    "no_div_data_marker": 1,
    "no_div_data_period": 5
}
```

### Long response

Header:
https://www.interactivebrokers.ie/tws.proxy/fundamentals/dividends/237937002

Response:
```
{
    "history": {
        "legends": [
            {
                "represent": "ASDATE",
                "display": "BOTTOM",
                "labels": [],
                "min": 1613948340000,
                "max": 1771718400,
                "title": "Calendar",
                "axis": "HORIZONTAL"
            },
            {
                "represent": "ASNUMBER",
                "display": "RIGHT",
                "labels": [
                    {
                        "hilight": true,
                        "text": "419.96",
                        "value": 419.9628573704165
                    },
                    {
                        "hilight": true,
                        "text": "566.28",
                        "value": 566.2761164881313
                    },
                    {
                        "hilight": true,
                        "text": "712.59",
                        "value": 712.589375605846
                    },
                    {
                        "hilight": true,
                        "text": "858.9",
                        "value": 858.9026347235608
                    },
                    {
                        "hilight": true,
                        "text": "1.01K",
                        "value": 1005.2158938412756
                    },
                    {
                        "hilight": true,
                        "text": "1.15K",
                        "value": 1151.5291529589904
                    }
                ],
                "min": 466.62539707824055,
                "max": 1046.8446845081733,
                "title": "Price",
                "axis": "VERTICAL"
            },
            {
                "represent": "ASNUMBER",
                "display": "LEFT",
                "labels": [
                    {
                        "hilight": true,
                        "text": "1.15",
                        "value": 1.1500092
                    },
                    {
                        "hilight": true,
                        "text": "1.36",
                        "value": 1.35854832
                    },
                    {
                        "hilight": true,
                        "text": "1.57",
                        "value": 1.5670874399999999
                    },
                    {
                        "hilight": true,
                        "text": "1.78",
                        "value": 1.7756265599999999
                    },
                    {
                        "hilight": true,
                        "text": "1.98",
                        "value": 1.9841656799999998
                    },
                    {
                        "hilight": true,
                        "text": "2.19",
                        "value": 2.1927048
                    }
                ],
                "min": 1.277788,
                "max": 1.993368,
                "title": "Dividends",
                "axis": "VERTICAL"
            }
        ],
        "series": [
            {
                "title": "Dividends",
                "name": "dividends",
                "plotData": [
                    {
                        "x": 1616112000000,
                        "y": 1.277788,
                        "type": "ACTUAL",
                        "amount": 1.277788,
                        "ex_dividend_date": {
                            "d": 19,
                            "m": "MAR",
                            "y": 2021
                        },
                        "formatted_amount": "1.28 USD",
                        "description": "Regular Dividend",
                        "declaration_date": {
                            "d": 19,
                            "m": "MAR",
                            "y": 2021
                        },
                        "record_date": {
                            "d": 22,
                            "m": "MAR",
                            "y": 2021
                        },
                        "payment_date": {
                            "d": 12,
                            "m": "MAY",
                            "y": 2021
                        }
                    },
                    {
                        "x": 1623974400000,
                        "y": 1.375875,
                        "type": "ACTUAL",
                        "amount": 1.375875,
                        "ex_dividend_date": {
                            "d": 18,
                            "m": "JUN",
                            "y": 2021
                        },
                        "formatted_amount": "1.38 USD",
                        "description": "Regular Dividend",
                        "declaration_date": {
                            "d": 18,
                            "m": "JUN",
                            "y": 2021
                        },
                        "record_date": {
                            "d": 21,
                            "m": "JUN",
                            "y": 2021
                        },
                        "payment_date": {
                            "d": 11,
                            "m": "AUG",
                            "y": 2021
                        }
                    },
                    ...
                    {
                        "x": 1766102400000,
                        "y": 1.993368,
                        "type": "ACTUAL",
                        "amount": 1.993368,
                        "ex_dividend_date": {
                            "d": 19,
                            "m": "DEC",
                            "y": 2025
                        },
                        "formatted_amount": "1.99 USD",
                        "description": "Regular Dividend",
                        "declaration_date": {
                            "d": 19,
                            "m": "DEC",
                            "y": 2025
                        },
                        "record_date": {
                            "d": 22,
                            "m": "DEC",
                            "y": 2025
                        },
                        "payment_date": {
                            "d": 13,
                            "m": "FEB",
                            "y": 2026
                        }
                    }
                ],
                "xlegend": "Calendar",
                "ylegend": "Dividends"
            },
            {
                "title": "Price",
                "name": "price",
                "plotData": [
                    {
                        "x": 1613948340000,
                        "y": 473.22429485511105,
                        "open": 476.15500997807527,
                        "close": 473.22429485511105,
                        "high": 476.15500997807527,
                        "low": 473.02316734667227,
                        "debugY": 20210221
                    },
                    {
                        "x": 1614034740000,
                        "y": 470.41808723737074,
                        "open": 478.87502009219895,
                        "close": 470.41808723737074,
                        "high": 478.87502009219895,
                        "low": 465.4760970300192,
                        "debugY": 20210222
                    },
                    ...
                    {
                        "x": 1771455540000,
                        "y": 972.62,
                        "open": 967.5,
                        "close": 972.62,
                        "high": 977.25,
                        "low": 967.5,
                        "debugY": 20260218
                    },
                    {
                        "x": 1771541940000,
                        "y": 975.52,
                        "open": 970.3100000000001,
                        "close": 975.52,
                        "high": 976.64,
                        "low": 969.69,
                        "debugY": 20260219
                    }
                ],
                "xlegend": "Calendar",
                "ylegend": "Price"
            }
        ]
    },
    "industry_comparison": {
        "content": [
            {
                "title": "Dividend Yield TTM",
                "title_tag": "Dividend_Yield_TTM",
                "value": 0.008496586146673148,
                "formatted_value": "0.85%",
                "search_id": "div_yield"
            },
            {
                "title": "Dividend TTM",
                "title_tag": "Dividend_TTM",
                "value": 5.585599,
                "formatted_value": "5.59",
                "search_id": "div_per_share"
            }
        ],
        "title": "Industry Comparison",
        "title_tag": "Industry_Comparison"
    },
    "no_div_data_marker": 2,
    "last_payed_dividend_date": {
        "d": 19,
        "m": "DEC",
        "y": 2025,
        "t": "2025-12-19"
    },
    "last_payed_dividend_amount": 1.993368,
    "last_payed_dividend_currency": "USD",
    "formatted_last_payed_dividend_amount": "1.99"
}
```

