# Products

Does not require a logged-in session state. max pageSize == 500. pageNumber=0 returns the same entries as pageNumber=1.

Request:
```
curl -X POST "https://www.interactivebrokers.ie/webrest/search/products-by-filters" \
     -H "Content-Type: application/json" \
     -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" \
     -d '{
           "domain": "ie",
           "newProduct": "all",
           "pageNumber": 0,
           "pageSize": 10,
           "productCountry":[],
           "productSymbol": "",
           "productType": ["ETF"],
           "sortDirection": "asc",
           "sortField": "symbol"
         }'
```

Response:
```
{
    "products": [
        {
            "type": "ETF",
            "symbol": "00400A",
            "exchangeId": "TWSE",
            "localSymbol": "00400A",
            "description": "CATHAY TAIWAN HIGH DVD MM",
            "conid": 871568621,
            "underConid": null,
            "isin": "TW00000400A3",
            "cusip": "Y122F7558",
            "currency": "TWD",
            "country": "TW",
            "isPrimeExchId": "T",
            "isNewPdt": "F",
            "assocEntityId": null,
            "fcConid": 1
        },
        ...
        {
            "type": "ETF",
            "symbol": "0053",
            "exchangeId": "TWSE",
            "localSymbol": "0053",
            "description": "YUANTA/P-SHRS TW EL TECH ETF",
            "conid": 637688452,
            "underConid": null,
            "isin": "TW0000053008",
            "cusip": "Y697A9105",
            "currency": "TWD",
            "country": "TW",
            "isPrimeExchId": "T",
            "isNewPdt": "F",
            "assocEntityId": null,
            "fcConid": 1
        }
    ],
    "productTypeCount": [],
    "productCount": 0
}
```