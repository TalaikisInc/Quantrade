# Quantrade
[![Build Status](https://travis-ci.org/xenu256/Quantrade.svg?branch=master)](https://travis-ci.org/xenu256/Quantrade)

[Quantrade](https://quantrade.talaikis.com) is quantitative strategies portfolio index, generating trade signals from its portfolio.

## Status

Deprecated.

## What it does

It uses data server, running Metatrader 4 platforms and processing server that gets the data via WebDAV (requires [data_server](https://github.com/xenu256/data_server)), processes it and then displays generated content. It would query, when configured, data server periodically to generate trade signals automatically from created strategies.

## STATUS

It's a beta, because it was changed many times over those few months, requires code reviews, which will be done
after deciding how it will or should look when operable, and most importantly, - remaining validation functionality.

Public website does nothing currently, and I am sometimes working on more simplistic to end users approach with much higher trading frequency than this project.

## TODO

* ~~API~~
* ~~Mobile app.~~ (in progress)
* ~~MQL4 EA.~~ (dropped?)
* In/out sample.
* ~~Monte Carlo~~
* Indicators & strategies inside database/ user form.
* If above, then API enhancements.
* Tests.

## Requirements

* Two included brokers ([Ava trade](http://www.avatrade.com/?tag=1997) and [Pepperstone](https://pepperstone.com/?a_aid=quantrade)) with 25 strategies generate over 200,000 files, over 100 Gb of data, so it is resources hungry project.
* As Metatrader 4 is visual, it probably needs Windows server.
* It needs both MySQL (receiving symbol data from MT4) and Postgres (everything else) servers.
* Python 3.6+, Nginx, Redis, aio or cherry or uWsgi (original).

## Technologies

* [Python](https://github.com/python/cpython) 3.6+
* [Django](https://github.com/django/django) 1.11+
* [uWSGI](https://github.com/unbit/uwsgi)
* [python-social-auth](https://github.com/omab/python-social-auth)
* [pandas](https://github.com/pandas-dev/pandas)
* [numpy](https://github.com/numpy/numpy)
* [matplotlib](https://github.com/matplotlib/matplotlib)
* [Plotly](https://github.com/plotly/plotly.py)
* [python-dotenv](https://github.com/theskumar/python-dotenv)
* ~~[Sanic](https://github.com/channelcat/sanic)~~
* ~~[asyncpg](https://github.com/MagicStack/asyncpg)~~
* [Quandl](https://github.com/quandl/quandl-python)
* Redis
* Nginx
* [arch](https://github.com/bashtage/arch)
* etc.

## APIs

* [quantrade_goapi v2](https://github.com/xenu256/quantrade_goapi)
* [quantrade_pyapi v1 (dropped)](https://github.com/xenu256/quantrade_pyapi)

## Mobile

* [quanion](https://github.com/xenu256/quanion)

## Differences between open source and actual

Open source repo doesn't include actual strategies.

## How to create strategies

It uses pandas, so it's pretty straightforward vectorized testing for simple strategies. First you need some indicator. Both indicator and strategy should be defined in utils.py and programmed inside collector/arctic_utils.py (IndicatorBase and SignalBase classes). Indicator example:

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

If you need to access broker, symbol, system or period on your strategy - use ``self.info["broker"]``,...

## Issues

* Yahoo dropped its data and as a concequence Quandl too. Project has some (very poor in my opinion) initial try to implement
something portfolio strategies related, but this issue prevents the intention to develop portfolio strategies further.

## "Special" features

* It implements some concept of "automarketing", when results for each trade and month are sent to social networks, RSS, email.
* As always, Nginx SSL config with A level security, big names don't usually do that.

## How to start

1. Setup and configure everything (biggest part).
Then easy:
2. python utils.py --setup=y
3. python utils.py --hourly=y
4. python utils.py --daily=y
5. python utils.py --monthly=y
6. You have your site.

## History

It was created as part of Project 10x. Project 10x was started several years ago in order to challenge myself and find interesting solutions for generating 10x profits (more exactly, goal is 25% per month or 1200%/year) for retail traders, who are limited in many various ways, like higher commissions, unreliable brokers, lack of knowledge what really works, etc. Although, Quantrade didn't find anything in similar order, it also found a promising concept of index of strategies.

* 2016.10-2017.02. At first I thought people would construct their own portfolios of strategies, but after release test, that appeared too complex for users, so
* 2017.03-2017.04 following version created indexing concept. That simplified things, but still not much.
* 2017.05+ - dropped active development after realizing no one of end-users gets it and decided to explore other concepts.

## Limitations

* Monte Carlo only works on Close values.