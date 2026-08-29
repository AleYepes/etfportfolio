## Sentiment series

Header:
https://www.interactivebrokers.ie/tws.proxy/sma/request?type=search&conid=237937002&from=1995-01-01%2012:40&to=2026-02-21%2012:40&bar_size=1D

The bar_size and date range query params can vary, but we want to keep daily bars and the range starting at 95. The response automatically shortens to start at the earliers available date.

Response:
```
{
    "sentiment": [
        {
            "sdispersion": 0.607,
            "svscore": -1.6055, # Tweet volume score
            "sdelta": -0.0392,
            "datetime": 1613883600000,
            "svolatility": 0.7024,
            "high": 476.155009978075,
            "low": 473.023167346672,
            "price": 473.224294855111,
            "svolume": 112,
            "close": 473.224294855111, # default price
            "open": 476.155009978075,
            "sscore": -1.2491, # Tweet sentiment score
            "smean": 1.28
        },
        {
            "price_change": 4.29072018002614,
            "sdispersion": 0.505,
            "svscore": 0.9938,
            "sdelta": -0.1062,
            "datetime": 1614056400000,
            "svolatility": 0.6603,
            "high": 470.197804728128,
            "low": 467.382019609986,
            "price": 468.933574675085,
            "price_change_p": 0.914995302479482,
            "svolume": 527,
            "close": 468.933574675085,
            "open": 470.197804728128,
            "sscore": -0.2911,
            "smean": 1.1532
        },
        {
            "price_change": 1.69521757112636,
            "sdispersion": 0.418,
            "svscore": 1.2905,
            "sdelta": -0.0212,
            "datetime": 1614229200000,
            "svolatility": 0.7738,
            "high": 476.767970003793,
            "low": 464.508769489433,
            "price": 467.238357103959,
            "price_change_p": 0.362816439479344,
            "svolume": 618,
            "close": 467.238357103959,
            "open": 476.767970003793,
            "sscore": 2.6996,
            "smean": 1.1509
        },
        {
            "datetime": 1614402000000,
            "svolatility": 0.8483,
            "sdispersion": 0.44,
            "svscore": 1.5392,
            "svolume": 778,
            "sdelta": -0.0239,
            "sscore": -0.41,
            "smean": 0.9204
        },
        {
            "price_change": -12.4028630203879,
            "sdispersion": 0.57,
            "svscore": -0.8695,
            "sdelta": -0.0053,
            "datetime": 1614574800000,
            "svolatility": 0.9257,
            "high": 481.556720204715,
            "low": 479.018682598227,
            "price": 479.641220124347,
            "price_change_p": -2.5858626198083,
            "svolume": 221,
            "close": 479.641220124347,
            "open": 481.556720204715,
            "sscore": -0.3183,
            "smean": 0.8272
        },
        {
            "price_change": 13.0158230461059,
            "sdispersion": 0.447,
            "svscore": 0.4046,
            "sdelta": 0.0897,
            "datetime": 1614747600000,
            "svolatility": 1.0143,
            "high": 476.499799992542,
            "low": 465.801732043682,
            "price": 466.625397078241,
            "price_change_p": 2.78935161429362,
            "svolume": 524,
            "close": 466.625397078241,
            "open": 476.499799992542,
            "sscore": -0.2728,
            "smean": 0.9024
        },
        ...
        {
            "price_change": -2.8999999999999773,
            "sdispersion": 0.4017595552467,
            "svscore": 0.43059242529534425,
            "sbuzz": 1.5178863099374544,
            "sdelta": -0.0038222376650451787,
            "datetime": 1771477200000,
            "svolatility": 1.7422417651146602,
            "high": 976.64,
            "low": 969.69,
            "price": 975.52,
            "price_change_p": -0.29727734951615314,
            "svolume": 1035.1646977067426,
            "close": 975.52,
            "open": 970.3100000000001,
            "sscore": 0.3973481584433641,
            "smean": 1.1370299513551079
        },
        {
            "datetime": 1771563600000,
            "svolatility": 1.73575,
            "sdispersion": 0.373,
            "svscore": 0.49395,
            "sbuzz": 1.6033,
            "svolume": 1069,
            "sdelta": -0.2448,
            "sscore": -0.23795,
            "smean": 1.0806
        },
        {
            "datetime": 1771650000000,
            "svolatility": 1.7783499999999999,
            "sdispersion": 0.3807512562814065,
            "svscore": 0.750520603015076,
            "sbuzz": 1.7732567839195943,
            "svolume": 1212.281407035175,
            "sdelta": 0.005522361809045202,
            "sscore": 0.5566027638190957,
            "smean": 1.1194452261306536
        }
    ]
}
```

