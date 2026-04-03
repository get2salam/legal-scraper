# PLS Data Audit Report - 2026-02-21

## 1. Executive Summary

**Overall Health Score: 99.7%**

- **Total cases in dataset:** 156,304
- **Reporters:** 10/10
- **Year coverage:** 1947-2026 (80 distinct years)
- **Zero corrupt JSON files** found in random sample
- **0 login page placeholder files** (scraper captured PLS login instead of judgment)
- **442 reporter/year combos** with >10% citation gaps (max_citation >> file_count)
- **18 missing year directories** across all reporters
- **Format completeness:** 100% original HTML coverage, 100% readable HTML coverage

### Critical Issues

1. **Login Page Contamination:** 0 files contain the PLS website login/placeholder page instead of actual judgment text. These need re-scraping.
2. **Citation Gaps:** Most reporter/year combinations show the file count is significantly less than the max citation number, indicating many cases were not scraped. Average coverage appears to be ~10-15% of available cases for many reporters.
3. **Missing Years:** Key gaps include PLD 1987, PCrLJ 1972/1978 (1 file each), PTD missing 12 years, PLC missing 4 years.
4. **Suspicious Duplicates:** 20 groups of 5+ files with identical file sizes found (especially SCMR 2020-2021 with 300+ login page files).

### Reporter Summary

| Reporter | Cases | Year Range | Years | Avg/Year | Login Pages |
|----------|------:|-----------|------:|---------:|------------:|
| SCMR | 25,525 | 1968-2026 | 59 | 433 | 0 |
| PLD | 21,666 | 1947-2026 | 79 | 274 | 0 |
| PCrLJ | 24,182 | 1968-2025 | 57 | 424 | 0 |
| MLD | 20,722 | 1984-2025 | 42 | 493 | 0 |
| CLC | 19,464 | 1979-2025 | 47 | 414 | 0 |
| YLR | 17,849 | 1999-2025 | 27 | 661 | 0 |
| PTD | 14,708 | 1960-2025 | 54 | 272 | 0 |
| PLC | 7,419 | 1970-2025 | 52 | 143 | 0 |
| CLD | 4,556 | 2002-2025 | 24 | 190 | 0 |
| GBLR | 213 | 2014-2016 | 3 | 71 | 0 |

## 2. Per-Year Breakdown

| Year | Total | SCMR | PLD | PCrLJ | MLD | CLC | YLR | PTD | PLC | CLD | GBLR | Issues |
|-----:|------:|-----:|----:|------:|----:|----:|----:|----:|----:|----:|-----:|--------|
| 1947 | 68 | - | 68 | - | - | - | - | - | - | - | - |  |
| 1948 | 57 | - | 57 | - | - | - | - | - | - | - | - |  |
| 1949 | 151 | - | 151 | - | - | - | - | - | - | - | - |  |
| 1950 | 159 | - | 159 | - | - | - | - | - | - | - | - |  |
| 1951 | 147 | - | 147 | - | - | - | - | - | - | - | - |  |
| 1952 | 226 | - | 226 | - | - | - | - | - | - | - | - |  |
| 1953 | 122 | - | 122 | - | - | - | - | - | - | - | - | >30% DROP |
| 1954 | 228 | - | 228 | - | - | - | - | - | - | - | - |  |
| 1955 | 206 | - | 206 | - | - | - | - | - | - | - | - |  |
| 1956 | 337 | - | 337 | - | - | - | - | - | - | - | - |  |
| 1957 | 417 | - | 417 | - | - | - | - | - | - | - | - |  |
| 1958 | 341 | - | 341 | - | - | - | - | - | - | - | - |  |
| 1959 | 388 | - | 388 | - | - | - | - | - | - | - | - |  |
| 1960 | 682 | - | 497 | - | - | - | - | 185 | - | - | - |  |
| 1961 | 402 | - | 402 | - | - | - | - | - | - | - | - | >30% DROP; Missing PTD |
| 1962 | 395 | - | 395 | - | - | - | - | - | - | - | - | Missing PTD |
| 1963 | 604 | - | 461 | - | - | - | - | 143 | - | - | - |  |
| 1964 | 493 | - | 386 | - | - | - | - | 107 | - | - | - |  |
| 1965 | 469 | - | 357 | - | - | - | - | 112 | - | - | - |  |
| 1966 | 554 | - | 465 | - | - | - | - | 89 | - | - | - |  |
| 1967 | 419 | - | 419 | - | - | - | - | - | - | - | - | Missing PTD |
| 1968 | 1394 | 497 | 267 | 511 | - | - | - | 119 | - | - | - |  |
| 1969 | 1331 | 416 | 364 | 434 | - | - | - | 117 | - | - | - |  |
| 1970 | 1292 | 414 | 332 | 303 | - | - | - | - | 243 | - | - | Missing PTD |
| 1971 | 1239 | 294 | 344 | 276 | - | - | - | 154 | 171 | - | - |  |
| 1972 | 771 | 281 | 251 | 1 | - | - | - | 89 | 149 | - | - | >30% DROP |
| 1973 | 839 | 199 | 261 | 209 | - | - | - | 85 | 85 | - | - |  |
| 1974 | 618 | 218 | 186 | 140 | - | - | - | - | 74 | - | - | Missing PTD |
| 1975 | 1017 | 242 | 446 | 329 | - | - | - | - | - | - | - | Missing PTD; Missing PLC |
| 1976 | 1411 | 263 | 463 | 377 | - | - | - | - | 308 | - | - | Missing PTD |
| 1977 | 1162 | 147 | 347 | 432 | - | - | - | - | 236 | - | - | Missing PTD |
| 1978 | 944 | 186 | 497 | 1 | - | - | - | - | 260 | - | - | Missing PTD |
| 1979 | 1321 | 269 | 382 | 267 | - | 223 | - | - | 180 | - | - | Missing PTD |
| 1980 | 1565 | 361 | 271 | 331 | - | 492 | - | 110 | - | - | - | Missing PLC |
| 1981 | 1912 | 533 | 288 | 353 | - | 434 | - | 99 | 205 | - | - |  |
| 1982 | 2454 | 581 | 355 | 396 | - | 812 | - | 104 | 206 | - | - |  |
| 1983 | 3008 | 603 | 260 | 767 | - | 971 | - | 95 | 312 | - | - |  |
| 1984 | 3766 | 728 | 228 | 935 | 512 | 854 | - | 111 | 398 | - | - |  |
| 1985 | 3621 | 862 | 254 | 942 | 615 | 706 | - | 166 | 76 | - | - |  |
| 1986 | 4384 | 918 | 218 | 1045 | 962 | 741 | - | 156 | 344 | - | - |  |
| 1987 | 369 | 366 | - | - | 2 | 1 | - | - | - | - | - | >50% DROP; Missing PLD; Missing PCrLJ; Missing PTD; Missing PLC |
| 1988 | 2966 | 986 | 298 | 328 | 646 | 708 | - | - | - | - | - | Missing PTD; Missing PLC |
| 1989 | 4972 | 1195 | 285 | 815 | 1510 | 617 | - | 252 | 298 | - | - |  |
| 1990 | 2870 | 797 | 261 | 506 | 446 | 435 | - | 178 | 247 | - | - | >30% DROP |
| 1991 | 3499 | 850 | 286 | 702 | 634 | 591 | - | 234 | 202 | - | - |  |
| 1992 | 3061 | 390 | 273 | 719 | 585 | 598 | - | 295 | 201 | - | - |  |
| 1993 | 3229 | 473 | 332 | 722 | 580 | 572 | - | 276 | 274 | - | - |  |
| 1994 | 3470 | 645 | 296 | 858 | 636 | 563 | - | 251 | 221 | - | - |  |
| 1995 | 2702 | 579 | 267 | 588 | 460 | 387 | - | 195 | 226 | - | - |  |
| 1996 | 2449 | 536 | 267 | 505 | 435 | 330 | - | 167 | 209 | - | - |  |
| 1997 | 2980 | 445 | 260 | 502 | 792 | 373 | - | 388 | 220 | - | - |  |
| 1998 | 2864 | 690 | 190 | 457 | 403 | 372 | - | 613 | 139 | - | - |  |
| 1999 | 3905 | 549 | 161 | 506 | 750 | 391 | 708 | 711 | 129 | - | - |  |
| 2000 | 3514 | 478 | 173 | 461 | 426 | 383 | 786 | 637 | 170 | - | - |  |
| 2001 | 3879 | 400 | 228 | 502 | 489 | 397 | 874 | 826 | 163 | - | - |  |
| 2002 | 4062 | 483 | 245 | 390 | 472 | 360 | 1082 | 631 | 68 | 331 | - |  |
| 2003 | 3914 | 488 | 322 | 460 | 489 | 391 | 875 | 551 | 87 | 251 | - |  |
| 2004 | 3618 | 573 | 332 | 494 | 484 | 330 | 688 | 380 | 44 | 293 | - |  |
| 2005 | 3787 | 464 | 280 | 327 | 496 | 362 | 987 | 414 | 86 | 371 | - |  |
| 2006 | 4263 | 492 | 333 | 445 | 507 | 452 | 1118 | 511 | 117 | 288 | - |  |
| 2007 | 3267 | 452 | 260 | 456 | 541 | 393 | 634 | 417 | 108 | 6 | - |  |
| 2008 | 3708 | 534 | 292 | 388 | 450 | 387 | 977 | 409 | 72 | 199 | - |  |
| 2009 | 2453 | 299 | 229 | 350 | 376 | 256 | 606 | 271 | 58 | 8 | - | >30% DROP |
| 2010 | 3273 | 311 | 198 | 432 | 481 | 407 | 1035 | 331 | 69 | 9 | - |  |
| 2011 | 3365 | 450 | 268 | 394 | 429 | 348 | 824 | 361 | 74 | 217 | - |  |
| 2012 | 2966 | 274 | 237 | 362 | 455 | 319 | 746 | 270 | 90 | 213 | - |  |
| 2013 | 2751 | 214 | 220 | 331 | 396 | 315 | 687 | 284 | 70 | 234 | - |  |
| 2014 | 2562 | 196 | 223 | 346 | 415 | 303 | 552 | 213 | 70 | 197 | 47 |  |
| 2015 | 2400 | 178 | 186 | 292 | 345 | 268 | 493 | 294 | 46 | 218 | 80 |  |
| 2016 | 2722 | 251 | 234 | 342 | 433 | 263 | 509 | 275 | 83 | 246 | 86 |  |
| 2017 | 2298 | 238 | 213 | 246 | 414 | 234 | 445 | 266 | 40 | 202 | - | Missing GBLR |
| 2018 | 2255 | 194 | 220 | 259 | 397 | 248 | 482 | 233 | 46 | 176 | - | Missing GBLR |
| 2019 | 2194 | 241 | 217 | 222 | 347 | 284 | 442 | 238 | 33 | 170 | - | Missing GBLR |
| 2020 | 2328 | 320 | 226 | 259 | 344 | 290 | 429 | 271 | 44 | 145 | - | Missing GBLR |
| 2021 | 2262 | 341 | 225 | 256 | 362 | 296 | 366 | 237 | 36 | 143 | - | Missing GBLR |
| 2022 | 2191 | 330 | 218 | 243 | 359 | 257 | 371 | 248 | 27 | 138 | - | Missing GBLR |
| 2023 | 1999 | 280 | 209 | 212 | 300 | 254 | 373 | 172 | 31 | 168 | - | Missing GBLR |
| 2024 | 1947 | 266 | 235 | 213 | 278 | 240 | 353 | 145 | 41 | 176 | - | Missing GBLR |
| 2025 | 2071 | 264 | 220 | 243 | 269 | 256 | 407 | 222 | 33 | 157 | - | Missing GBLR |
| 2026 | 5 | 1 | 4 | - | - | - | - | - | - | - | - | >50% DROP; Missing PCrLJ; Missing MLD; Missing CLC; Missing YLR; Missing PTD; Missing PLC; Missing CLD; Missing GBLR |

## 3. Citation Gap Analysis

Years where (max_citation - file_count) > 10% of max_citation, indicating missing cases.

**Note:** Citation numbers on PLS are page numbers, not sequential case numbers. A max citation of 2000 with 300 files means we have ~300 cases spanning pages 1-2000 of that year's volume.

| Reporter | Year | Files | Max Citation | Gap | Gap % |
|----------|-----:|------:|------------:|----:|------:|
| PCrLJ | 1972 | 1 | 756 | 755 | 99.9% |
| CLC | 1987 | 1 | 1962 | 1961 | 99.9% |
| MLD | 1987 | 2 | 674 | 672 | 99.7% |
| CLD | 2007 | 6 | 1465 | 1459 | 99.6% |
| CLD | 2010 | 9 | 1572 | 1563 | 99.4% |
| PCrLJ | 1978 | 1 | 150 | 149 | 99.3% |
| PLC | 2025 | 33 | 1065 | 1032 | 96.9% |
| PLC | 2010 | 69 | 1424 | 1355 | 95.2% |
| PLC | 1985 | 76 | 1085 | 1009 | 93.0% |
| CLD | 2025 | 157 | 1918 | 1761 | 91.8% |
| CLD | 2022 | 138 | 1566 | 1428 | 91.2% |
| SCMR | 2018 | 194 | 2128 | 1934 | 90.9% |
| PTD | 2024 | 145 | 1584 | 1439 | 90.8% |
| PTD | 2018 | 233 | 2508 | 2275 | 90.7% |
| PTD | 2023 | 172 | 1843 | 1671 | 90.7% |
| PTD | 2016 | 275 | 2936 | 2661 | 90.6% |
| SCMR | 2015 | 178 | 1813 | 1635 | 90.2% |
| CLD | 2020 | 145 | 1483 | 1338 | 90.2% |
| PTD | 2014 | 213 | 2144 | 1931 | 90.1% |
| CLD | 2021 | 143 | 1444 | 1301 | 90.1% |
| PCrLJ | 2024 | 213 | 2081 | 1868 | 89.8% |
| PTD | 2019 | 238 | 2325 | 2087 | 89.8% |
| CLD | 2013 | 234 | 2284 | 2050 | 89.8% |
| SCMR | 2014 | 196 | 1858 | 1662 | 89.5% |
| CLD | 2012 | 213 | 2032 | 1819 | 89.5% |
| CLD | 2016 | 246 | 2325 | 2079 | 89.4% |
| PTD | 2017 | 266 | 2490 | 2224 | 89.3% |
| CLD | 2023 | 168 | 1563 | 1395 | 89.3% |
| PTD | 2021 | 237 | 2192 | 1955 | 89.2% |
| CLD | 2015 | 218 | 2015 | 1797 | 89.2% |
| PTD | 1966 | 89 | 820 | 731 | 89.1% |
| PTD | 2015 | 294 | 2654 | 2360 | 88.9% |
| SCMR | 2013 | 214 | 1904 | 1690 | 88.8% |
| CLC | 2024 | 240 | 2134 | 1894 | 88.8% |
| CLD | 2024 | 176 | 1570 | 1394 | 88.8% |
| PLC | 2019 | 33 | 291 | 258 | 88.7% |
| CLD | 2017 | 202 | 1788 | 1586 | 88.7% |
| SCMR | 2017 | 238 | 2091 | 1853 | 88.6% |
| SCMR | 2016 | 251 | 2186 | 1935 | 88.5% |
| CLD | 2014 | 197 | 1715 | 1518 | 88.5% |
| PCrLJ | 2023 | 212 | 1834 | 1622 | 88.4% |
| PTD | 2025 | 222 | 1914 | 1692 | 88.4% |
| SCMR | 2019 | 241 | 2063 | 1822 | 88.3% |
| CLC | 2023 | 254 | 2169 | 1915 | 88.3% |
| CLD | 2018 | 176 | 1505 | 1329 | 88.3% |
| PCrLJ | 2025 | 243 | 2044 | 1801 | 88.1% |
| CLC | 2022 | 257 | 2136 | 1879 | 88.0% |
| CLD | 2011 | 217 | 1812 | 1595 | 88.0% |
| PTD | 2013 | 284 | 2344 | 2060 | 87.9% |
| PLC | 2024 | 41 | 338 | 297 | 87.9% |
| PTD | 2009 | 271 | 2219 | 1948 | 87.8% |
| PTD | 2020 | 271 | 2220 | 1949 | 87.8% |
| PCrLJ | 2019 | 222 | 1800 | 1578 | 87.7% |
| CLC | 2018 | 248 | 2020 | 1772 | 87.7% |
| CLC | 2025 | 256 | 2060 | 1804 | 87.6% |
| YLR | 2024 | 353 | 2857 | 2504 | 87.6% |
| PTD | 2010 | 331 | 2673 | 2342 | 87.6% |
| CLD | 2019 | 170 | 1374 | 1204 | 87.6% |
| PTD | 2011 | 361 | 2881 | 2520 | 87.5% |
| PLC | 2023 | 31 | 248 | 217 | 87.5% |
| SCMR | 2025 | 264 | 2103 | 1839 | 87.4% |
| PLD | 1998 | 190 | 1512 | 1322 | 87.4% |
| PTD | 1968 | 119 | 947 | 828 | 87.4% |
| PLC | 2022 | 27 | 214 | 187 | 87.4% |
| SCMR | 2024 | 266 | 2071 | 1805 | 87.2% |
| PTD | 2022 | 248 | 1942 | 1694 | 87.2% |
| SCMR | 2023 | 280 | 2165 | 1885 | 87.1% |
| PTD | 1969 | 117 | 908 | 791 | 87.1% |
| PTD | 1964 | 107 | 824 | 717 | 87.0% |
| PCrLJ | 2022 | 243 | 1852 | 1609 | 86.9% |
| CLC | 2017 | 234 | 1793 | 1559 | 86.9% |
| PTD | 1965 | 112 | 845 | 733 | 86.7% |
| MLD | 2025 | 269 | 2010 | 1741 | 86.6% |
| PTD | 1960 | 185 | 1385 | 1200 | 86.6% |
| PTD | 1995 | 195 | 1450 | 1255 | 86.6% |
| PLC | 2021 | 36 | 268 | 232 | 86.6% |
| SCMR | 2012 | 274 | 2008 | 1734 | 86.4% |
| PCrLJ | 2021 | 256 | 1887 | 1631 | 86.4% |
| CLC | 2016 | 263 | 1936 | 1673 | 86.4% |
| CLC | 2019 | 284 | 2083 | 1799 | 86.4% |

*...and 362 more entries*


**Total reporter/year combos with >10% citation gaps: 442**

## 4. Integrity Results

Random sample of 1323 files checked (3 per reporter/year):

- **Valid & complete:** 1318 (99.6%)
- **Corrupt JSON:** 0
- **Stub files (<500 bytes):** 0
- **Missing fields (no case_name/title):** 0
- **Empty judgment:** 0
- **Login page placeholders (in sample):** 5

### Login Page Contamination (Full Scan)

**Total login page files across entire dataset: 0**


### Sample Integrity Issues (first 30)

| Reporter | Year | File | Size | Issues |
|----------|------|------|-----:|--------|
| SCMR | 2020 | 2020_SCMR_1970.json | 64,189 | LOGIN PAGE (not real judgment) |
| SCMR | 2021 | 2021_SCMR_1795.json | 64,190 | LOGIN PAGE (not real judgment) |
| SCMR | 2021 | 2021_SCMR_1387.json | 64,189 | LOGIN PAGE (not real judgment) |
| PLD | 2006 | 2006_PLD_331.json | 64,186 | LOGIN PAGE (not real judgment) |
| MLD | 2024 | 2024_MLD_474.json | 64,188 | LOGIN PAGE (not real judgment) |

## 5. Format Coverage

- **Reporter/year directories with original/ HTML folder:** 444/444 (100%)
- **HTML file count matches JSON count:** 444/444

### Readable HTML (data_v2/html/)

| Reporter | Years | HTML Files |
|----------|------:|-----------:|
| SCMR | 59 | 25,525 |
| PLD | 79 | 21,666 |
| PCrLJ | 57 | 24,182 |
| MLD | 42 | 20,722 |
| CLC | 47 | 19,464 |
| YLR | 27 | 17,849 |
| PTD | 54 | 14,708 |
| PLC | 52 | 7,419 |
| CLD | 24 | 4,556 |
| GBLR | 3 | 213 |

## 6. Anomalies

### Year-over-Year Drops

| Year | Total | Prev Year Total | Drop |
|-----:|------:|----------------:|------|
| 1953 | 122 | 226 | -46.0% (>30% DROP) |
| 1961 | 402 | 682 | -41.1% (>30% DROP) |
| 1972 | 771 | 1239 | -37.8% (>30% DROP) |
| 1987 | 369 | 4384 | -91.6% (>50% DROP) |
| 1990 | 2870 | 4972 | -42.3% (>30% DROP) |
| 2009 | 2453 | 3708 | -33.8% (>30% DROP) |
| 2026 | 5 | 2071 | -99.8% (>50% DROP) |

### Missing Year Directories

- **PCrLJ:** [1987]
- **PLC:** [1975, 1980, 1987, 1988]
- **PLD:** [1987]
- **PTD:** [1961, 1962, 1967, 1970, 1974, 1975, 1976, 1977, 1978, 1979, 1987, 1988]

### Duplicate Citation Numbers

**No duplicate citation numbers found.** Good.


### Suspicious Identical File Sizes (5+ files same size)

| Reporter | Year | Size (bytes) | Count | Sample Files |
|----------|-----:|------------:|------:|-------------|
| SCMR | 1988 | 11,277 | 7 | 1988_SCMR_2011.json, 1988_SCMR_2015.json |
| SCMR | 2016 | 64,189 | 24 | 2016_SCMR_2081.json, 2016_SCMR_2082.json |
| SCMR | 2020 | 64,189 | 71 | 2020_SCMR_1418.json, 2020_SCMR_1425.json |
| SCMR | 2020 | 64,192 | 6 | 2020_SCMR_1437.json, 2020_SCMR_1523.json |
| SCMR | 2021 | 64,189 | 128 | 2021_SCMR_1008.json, 2021_SCMR_1016.json |
| SCMR | 2021 | 64,190 | 40 | 2021_SCMR_1795.json, 2021_SCMR_1797.json |
| SCMR | 2021 | 64,188 | 64 | 2021_SCMR_557.json, 2021_SCMR_558.json |
| MLD | 2024 | 64,188 | 25 | 2024_MLD_387.json, 2024_MLD_392.json |
| PTD | 2015 | 138,972 | 19 | 2015_PTD_1308.json, 2015_PTD_1356.json |
| PTD | 2015 | 139,000 | 19 | 2015_PTD_1330.json, 2015_PTD_1335.json |
| PTD | 2015 | 139,006 | 9 | 2015_PTD_1351.json, 2015_PTD_1457.json |
| PTD | 2015 | 138,973 | 6 | 2015_PTD_1354.json, 2015_PTD_1370.json |
| PTD | 2015 | 138,977 | 10 | 2015_PTD_1363.json, 2015_PTD_1591.json |
| PTD | 2015 | 139,002 | 16 | 2015_PTD_1385.json, 2015_PTD_1451.json |
| PTD | 2015 | 138,974 | 9 | 2015_PTD_1417.json, 2015_PTD_1524.json |
| PTD | 2015 | 138,975 | 6 | 2015_PTD_1422.json, 2015_PTD_1438.json |
| PTD | 2015 | 139,004 | 5 | 2015_PTD_1469.json, 2015_PTD_1580.json |
| PTD | 2015 | 139,005 | 5 | 2015_PTD_1490.json, 2015_PTD_1543.json |
| PTD | 2015 | 138,970 | 6 | 2015_PTD_1520.json, 2015_PTD_1749.json |
| PTD | 2015 | 139,007 | 7 | 2015_PTD_1847.json, 2015_PTD_2042.json |

### Known Problem Years (Special Attention)

- **PLD 1987:** MISSING files - Expected ~300+ cases, directory MISSING
- **PCrLJ 1972:** 1 files - Only 1 file (should have ~500+)
- **PCrLJ 1978:** 1 files - Only 1 file (should have ~400+)
- **CLC 1987:** 1 files - Only 1 file (should have ~800+)
- **MLD 1987:** 2 files - Only 2 files (should have ~500+)
- **SCMR 1987:** 366 files - 366 files - likely only partial

## 7. Recommendations

### Priority 1: CRITICAL - Re-scrape Login Page Files

**0 files** contain the PLS website placeholder instead of actual judgment content.
These files have valid citations but their `judgment_raw` field contains the PLS login/error page.
Re-scrape with proper session authentication.


### Priority 2: HIGH - Fill Missing Year Directories

- **PCrLJ:** Scrape years [1987]
- **PLC:** Scrape years [1975, 1980, 1987, 1988]
- **PLD:** Scrape years [1987]
- **PTD:** Scrape years [1961, 1962, 1967, 1970, 1974, 1975, 1976, 1977, 1978, 1979, 1987, 1988]

### Priority 3: HIGH - Fill Nearly-Empty Years

- **SCMR/2026:** Only 1 file(s)
- **PLD/2026:** Only 4 file(s)
- **PCrLJ/1972:** Only 1 file(s)
- **PCrLJ/1978:** Only 1 file(s)
- **MLD/1987:** Only 2 file(s)
- **CLC/1987:** Only 1 file(s)

### Priority 4: MEDIUM - Fill Citation Gaps

Most reporter/year combos show significant gaps between file count and max citation number.
The top 20 worst gaps (excluding anomalous CLD 2009):

- **PCrLJ/1972:** 1 files, max citation 756, ~755 cases potentially missing (99.9% gap)
- **CLC/1987:** 1 files, max citation 1962, ~1961 cases potentially missing (99.9% gap)
- **MLD/1987:** 2 files, max citation 674, ~672 cases potentially missing (99.7% gap)
- **CLD/2007:** 6 files, max citation 1465, ~1459 cases potentially missing (99.6% gap)
- **CLD/2010:** 9 files, max citation 1572, ~1563 cases potentially missing (99.4% gap)
- **PCrLJ/1978:** 1 files, max citation 150, ~149 cases potentially missing (99.3% gap)
- **PLC/2025:** 33 files, max citation 1065, ~1032 cases potentially missing (96.9% gap)
- **PLC/2010:** 69 files, max citation 1424, ~1355 cases potentially missing (95.2% gap)
- **PLC/1985:** 76 files, max citation 1085, ~1009 cases potentially missing (93.0% gap)
- **CLD/2025:** 157 files, max citation 1918, ~1761 cases potentially missing (91.8% gap)
- **CLD/2022:** 138 files, max citation 1566, ~1428 cases potentially missing (91.2% gap)
- **SCMR/2018:** 194 files, max citation 2128, ~1934 cases potentially missing (90.9% gap)
- **PTD/2024:** 145 files, max citation 1584, ~1439 cases potentially missing (90.8% gap)
- **PTD/2018:** 233 files, max citation 2508, ~2275 cases potentially missing (90.7% gap)
- **PTD/2023:** 172 files, max citation 1843, ~1671 cases potentially missing (90.7% gap)
- **PTD/2016:** 275 files, max citation 2936, ~2661 cases potentially missing (90.6% gap)
- **SCMR/2015:** 178 files, max citation 1813, ~1635 cases potentially missing (90.2% gap)
- **CLD/2020:** 145 files, max citation 1483, ~1338 cases potentially missing (90.2% gap)
- **PTD/2014:** 213 files, max citation 2144, ~1931 cases potentially missing (90.1% gap)
- **CLD/2021:** 143 files, max citation 1444, ~1301 cases potentially missing (90.1% gap)

### Priority 5: LOW - Investigate Duplicate-Size Files

SCMR 2020 (81 files) and SCMR 2021 (233 files) have identical file sizes (~64KB each).
These are confirmed login page placeholders - all contain the PLS website template instead of judgments.
Already covered by Priority 1 re-scraping.

---

*Report generated: 2026-02-21 | Total files scanned: 156,304 | Audit scripts in data_v2/audit/*