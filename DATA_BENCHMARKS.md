# Benchmarks for data type

Tasks include not optimized cycles between reading and writing ~60,000 files to/from disk. 
Total time includes not only reading-writing, but also almost everything real-life: 
database communication time and math-computation time, but still the biggest 
time consume is for disk operations. 
Tasks are mainly done through [pandas](https://github.com/pandas-dev/pandas) library.

This benchmark was made due to several reasons, 1) after initial pickle method selcted I wanted to 
double check if this still holds as the best choice; 2) to choose next best method for Golang based API, 
3) to verify claims of various methods authors, 4) because that's interesting. SHOW_DEBUG was enabled.

## Versions / time for hourly tasks

* v1-5 (Mysql, MongoDB, Postgres, InflucDB, arctic) - a from several horus to a several days.
* v6-8 - hdf, message pack, csv
* v9 - pickle, 10 min
* v10 - pickle async one-thread - 6-10 min
* v11 async + multithread

## Test system

Ubuntu 16.04 under Windows 10, Pentium G3220, 16 Gb RAM.

## Process types

* Hourly tasks - from incoming csv reading to end signals (without performance).
* Daily tasks - performance, (2) indexes and stats.

### List of some hourly processes:

1) Incoming 980 csv files (~36 Mb).
2) Pickling incoming files with some added columns (-> ~60 Mb).
3) Pickled initial data to indicators (13.5k for each format).
4) Indicators to strategies.
5) Strategies files to performance files.
6) Performance files to stats table.

## 2017-07-22

## Pickle, v11, async + 6 cores, live

* Hourly tasks - 740 s

## 2017-07-08

## Pickle, v10, live 6 cores, all tasks

* hourly tasks - 280 - 310 s
* daily tasks - 2100 - 2400 s, usual

## Pickle, v9

* hourly tasks - 518.62 s, 813.87 s (after other types)
* daily tasks - 2252.88

## Pickle, protocol 2

* hourly tasks - 1094.19 s
* daily tasks - 2443.04

## Json

FIXME. Conversion from float to Decimal not supported (stats part).

```python
decimal.Decimal(str(x)
```

* hourly tasks - 1289.01 s - 2026.45 s
* daily tasks - 2406.44 s

## Message pack

* hourly tasks - 1308.34 s
* daily tasks - 3554.5 s

## HDF

* hourly tasks - 1446.66 s
* daily tasks - 2601.91 s


## HDF one store

Extremely slow after 1 Gb of file size. Did't finished.

## Feather

TODO, there's an issue on feather's GitHUB, doesn't support indexes.