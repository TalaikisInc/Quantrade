# Quantrade

Quantrade (quantrade.co.uk) is quantitative strategies portfolio index, generating trade signals from its portfolio.

## What it does

It uses data server, running Metatrader 4 platforms and processing server that gets the data via WebDAV, processes it and then displays generated content. It would query when configured data server periodically generating trade signals automatically from created strategies.

## STATUS

It's a beta, because it was changed many times over those few months and requires code reviews, which will be done
after deciding how it will or should look when operable.

## TODO

* In/out sample.
* Monte Carlo.
* ~~Mobile app.~~ (dropped?)
* ~~MQL4 EA.~~ (dropped?)
* Indicators & strategies inside database/ user form.
* If above, then API enhancements.

## Requirements

* Two included brokers ([Ava trade](http://www.avatrade.com/?tag=1997) and [Pepperstone](https://pepperstone.com/?a_aid=quantrade)) with 25 strategies generate over 200,000 files, over 100 Gb of data, so it is resources hungry project.
* As Metatrader 4 is visual, it probably needs Windows server.
* It needs both MySQL (receiving symbol data from MT4) and Postgres (everything else) servers.

## Differences between open source and actual

Open source repo doesn't include actual strategies.

## How to create strategies

It uses pandas, so it's pretty straightforward for simple strategies. First you need some indicator. Both indicator and strategy should be defined in utils.py and programmed inside collector/arctic_utils.py (IndicatorBase and SignalBase classes). Indicator example:

```text
# "SMA" is an indicator name defined inside utils.py
# "per" variable is also defined inside utils.py
if 'SMA' in self.name:
    df['VALUE'] = df['CLOSE'] - df['CLOSE'].rolling(self.per, min_periods=self.per).mean()

    #following lines should be used for each indicator
    df = await self.clean_df(df)
    await self.insert(df=df)
```

And then you can query that indicator for your strategy:

```text
# "SM" is defined in utils.py as part of strategy name, it auto-generates
# 2 strategies at once - mean reversing and momentum, 3 sides (only longs, only shorts,
# longs & shorts)
# "BUY_SIDE" and "SELL_SIDE" columns should be used for each strategy
# indicator "VALUE" should be shifted, of course
if 'SM' in self.name:
    if self.mean_rev:
        df['BUY_SIDE'] = where(df['VALUE'].shift() < 0.0, 1, 0)
        df['SELL_SIDE'] = where(df['VALUE'].shift() > 0.0, -1, 0)
    else:
        df['BUY_SIDE'] = where(df['VALUE'].shift() > 0.0, 1, 0)
        df['SELL_SIDE'] = where(df['VALUE'].shift() < 0.0, -1, 0)

    #following lines should be used for each signal generator
    df = await self.clean_df(df=df)
    await self.insert(df.dropna())
```

## History

It was created as part of Project 10x. Project 10x was started several years ago in order to challenge myself and find interesting solutions for generating 10x profits (more exactly, goal is 25% per month or 1200%/year) for retail traders, that are limited in many various ways, like higher commissions, unreliable brokers, lack of knowledge what really works, etc. Although, Quantrade didn't find anything in similar order, it also found a promising concept of index of strategies.

* 2016.10-2017.02. At first I thought people would construct their own portfolios of strategies, but after release test, that appeared too complex for users, so
* 2017.03-2017.04 following version created indexing concept. That simplified things, but still not much.
* ...
