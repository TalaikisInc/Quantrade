from asyncio import gather
from os.path import join

from clint.textui import colored
from numpy import where, maximum, sum, empty, exp, log, round, sqrt

from django.conf import settings
from django.db import IntegrityError

from .models import Stats, Brokers, Symbols, Periods, Systems
from .utils import multi_filenames, df_multi_reader, name_deconstructor, file_cleaner


async def update_stats(broker, symbol, period, system, direction, stats):
    try:
        s = Stats.objects.get(broker=broker, symbol=symbol, \
            period=period, system=system, direction=direction)

        s.sharpe = stats['sharpe']
        s.bh_sharpe = stats['bh_sharpe']
        s.std = stats['std']
        s.var = stats['var']
        s.avg_trade = stats['avg_trade']
        s.avg_win = stats['avg_win']
        s.avg_loss = stats['avg_loss']
        s.win_rate = stats['win_rate']
        s.trades = stats['trades']
        s.fitness = stats['fr']
        s.intraday_dd = stats['intraday_dd']
        s.total_profit = stats['total_profit']
        s.max_dd = stats['max_dd']
        s.yearly = stats['yearly']
        s.yearly_p = stats['avg_yearly_p']
        s.acc_minimum = stats['acc_minimum']
        s.sortino = stats['sortino']
        s.bh_sortino = stats['bh_sortino']
        s.save()

        if settings.SHOW_DEBUG:
            print("Updated stats for {}\n".format(symbol))
    except Exception as err:
        print(colored.red("At update stats {}".format(err)))


async def std_func(data):
    try:
        std = 0
        std = data.loc[data != 0].std()
    except Exception as err:
        if settings.SHOW_DEBUG:
            print(colored.red("At std func {}\n".format(err)))
    return std


async def var_func(data):
    var = 0
    try:
        var = data.loc[data != 0].var()
    except Exception as e:
        if settings.SHOW_DEBUG:
            print(colored.red("At var func {}\n".format(e)))
    return var


async def avg_trade_func(data):
    try:
        avg_trade = 0
        data = data.fillna(0.0)
        avg_trade = data.loc[data != 0].mean()
    except Exception as e:
        if settings.SHOW_DEBUG:
            print(colored.red("At avg trade func {}\n".format(e)))
    return avg_trade


async def lpm(returns, threshold, order):
    """
    This method returns a lower partial moment of the returns
    Create an array he same length as returns containing the minimum return threshold
    """
    threshold_array = empty(len(returns))
    threshold_array.fill(threshold)
    # Calculate the difference between the threshold and the returns
    diff = threshold_array - returns
    # Set the minimum of each to 0
    diff = diff.clip(min=0)
    # Return the sum of the different to the power of order
    return sum(diff ** order) / len(returns)


async def sortino_ratio(returns, target=0):
    expected_return = returns.mean()
    risk_free = settings.RISK_FREE
    return (expected_return - risk_free) / sqrt(await lpm(returns, target, 2))


async def sharpe_func(avg_trade, std):
    try:
        sharpe = 0.0
        if not (avg_trade is None):
            if not (std is None):
                sharpe = avg_trade / std
            else:
                sharpe = 0.0
        else:
            sharpe = 0.0
    except Exception as e:
        if settings.SHOW_DEBUG:
            print(colored.red("At sharpe func {}\n".format(e)))
    return sharpe


async def avg_win_loss_func(data):
    try:
        avg_win = 0.0
        avg_loss = 0.0
        avg_win = data.loc[data > 0].mean()
        avg_loss = data.loc[data < 0].mean()
    except Exception as e:
        if settings.SHOW_DEBUG:
            print(colored.red("At avg win func {}\n".format(e)))
    return (avg_win, avg_loss)


async def win_rate_func(data):
    try:
        win_rate = float(data.loc[data > 0].count()) / data.loc[data != 0].count()
    except Exception as e:
        if settings.SHOW_DEBUG:
            print("At win rate func {}\n".format(e))
        win_rate = 0.0
    return win_rate


async def trades_func(data):
    try:
        trades = int(data.loc[data == 1].count())
    except Exception as e:
        if settings.SHOW_DEBUG:
            print("At trades func {}\n".format(e))
        trades = 0

    return trades


async def fr_func(avg_trade, trades, returns, cumsum):
    try:
        gross_profit = returns.loc[returns > 0].sum()
        gross_loss = abs(returns.loc[returns < 0].sum())
        total_profit = cumsum.ix[-1]
        fr = await fitness_rank(average_trade=avg_trade, trades=trades, \
            gross_profit=gross_profit, gross_loss=gross_loss)
    except Exception as e:
        if settings.SHOW_DEBUG:
            print("fr_func {}\n".format(e))
        fr = 0.0
        gross_profit = 0,0
        gross_loss = 0.0
        total_profit = 0.0
    return (gross_profit, gross_loss, total_profit, fr)


async def max_dd_func(data):
    try:
        max_y = maximum.accumulate(data)
        dd = data - max_y
        max_dd = abs(dd.min())
    except Exception as e:
        if settings.SHOW_DEBUG:
            print("max_dd_func {}\n".format(e))
        max_dd = None
    return max_dd


async def idd_func(df, direction):
    try:
        if direction == 1:
            filtered = df.loc[df['LONG_MAE'] != 0]
            intraday_dd = (filtered['LONG_PL_CUMSUM'] - filtered['LONG_MAE']).max()
        elif direction == 2:
            filtered = df.loc[df['SHORT_MAE'] != 0]
            intraday_dd = (filtered['SHORT_PL_CUMSUM'] - filtered['SHORT_MAE']).max()
        else:
            filtered = df.loc[(df['SHORT_MAE']+df['LONG_MAE']) != 0]
            intraday_dd = ((filtered['SHORT_PL_CUMSUM']+filtered['LONG_PL_CUMSUM']) - (filtered['SHORT_MAE']+filtered['LONG_MAE'])).max()
    except Exception as e:
        if settings.SHOW_DEBUG:
            print("At idd_func {}\n".format(e))
        intraday_dd = None
    return intraday_dd


async def yearly_func(total_profit, years, max_dd, intraday_dd, symbol, margin=0.0):
    try:
        yearly = total_profit / years
        if not(symbol is None):
            acc_minimum = max_dd + intraday_dd + float(symbol.margin_initial)
        else:
            acc_minimum = max_dd + intraday_dd + margin
        if acc_minimum != 0:
            avg_yearly_p = (yearly * 100.0) / (acc_minimum * 2.0)
        else:
            avg_yearly_p = 0.0
    except Exception as err:
        if settings.SHOW_DEBUG:
            print(colored.red("At yearly func {}\n".format(err)))
        yearly = 0.0
        acc_minimum = None
        avg_yearly_p = 0.0

    return (yearly, acc_minimum, avg_yearly_p)


async def get_stats(df, direction, years, symbol):
    try:
        stats = {}
        if direction == 1:
            returns = df['LONG_PL']
            diff = df['DIFF']
            trades = df['LONG_TRADES']
            cumsum = df['LONG_PL_CUMSUM']
            margin = df['LONG_MARGIN'].max()
        elif direction == 2:
            returns = df['SHORT_PL']
            diff = -df['DIFF']
            trades = df['SHORT_TRADES']
            cumsum = df['SHORT_PL_CUMSUM']
            margin = df['SHORT_MARGIN'].max()
        else:
            returns = df['BUYSELL']
            diff = df['DIFF']
            trades = df['SHORT_TRADES'] + df['LONG_TRADES']
            cumsum = df['BUYSELL_CUMSUM']
            margin = (df['LONG_MARGIN'] + df['SHORT_MARGIN']).max()

        stats['std'] = await std_func(data=returns)
        stats['margin'] = margin
        stats['var'] = await var_func(data=returns)
        stats['avg_trade'] = await avg_trade_func(data=returns)
        stats['sharpe'] = await sharpe_func(avg_trade=stats['avg_trade'], std=stats['std'])
        stats['sortino'] = await sortino_ratio(returns=returns.as_matrix(), target=0)
        stats['bh_std'] = await std_func(data=diff)
        stats['bh_avg_trade'] = await avg_trade_func(data=diff)
        stats['bh_sharpe'] = await sharpe_func(avg_trade=stats['bh_avg_trade'], std=stats['bh_std'])
        stats['bh_sortino'] = await sortino_ratio(returns=diff.as_matrix(), target=0)
        stats['avg_win'], stats['avg_loss'] = await avg_win_loss_func(data=returns)
        stats['win_rate'] = await win_rate_func(data=returns)
        stats['trades'] = await trades_func(data=trades)
        stats['gross_profit'], stats['gross_loss'], stats['total_profit'], \
            stats['fr'] = await fr_func(avg_trade=stats['avg_trade'], \
            trades=stats['trades'], returns=returns, cumsum=cumsum)
        stats['max_dd'] = await max_dd_func(data=cumsum)
        stats['intraday_dd'] = await idd_func(df=df, direction=direction)
        stats['yearly'], stats['acc_minimum'], stats['avg_yearly_p'] = await \
            yearly_func(total_profit=stats['total_profit'], years=years, \
            max_dd=stats['max_dd'], intraday_dd=stats['intraday_dd'], symbol=symbol)
    except Exception as err:
        print(colored.red("At get_stats {}".format(err)))

    return stats


async def write_stats(stats, broker, symbol, period, system, direction):
    try:
        if stats['trades'] >= settings.MIN_TRADES:
            try:
                s = Stats.objects.create(broker=broker, symbol=symbol, period=period, \
                    system=system, direction=direction, sharpe=stats['sharpe'], \
                    std=stats['std'], var=stats['var'], avg_trade=stats['avg_trade'], \
                    avg_win=stats['avg_win'], avg_loss=stats['avg_loss'], \
                    win_rate=stats['win_rate'], trades=stats['trades'], fitness=stats['fr'], \
                    intraday_dd=stats['intraday_dd'], max_dd=stats['max_dd'], \
                    total_profit=stats['total_profit'], yearly=stats['yearly'], yearly_p=stats['avg_yearly_p'], \
                    acc_minimum=stats['acc_minimum'], bh_sharpe=stats['bh_sharpe'], \
                    sortino=stats['sortino'], bh_sortino=stats['bh_sortino'])
                s.save()
                print(colored.green("Wrote new stats for {}".format(symbol)))
            except IntegrityError:
                pass
            except Exception as e:
                print(colored.red("At write_stats {}".format(e)))
                await update_stats(broker=broker, symbol=symbol, period=period, \
                    system=system, direction=direction, stats=stats)
    except Exception as err:
        print(colored.red("At writing stats {}".format(err)))


async def stats_process(df, d, years, broker, symbol, period, system):
    if settings.SHOW_DEBUG:
        print("Processing stats for {0} {1}".format(symbol, system))
    try:
        stats = await get_stats(df=df, direction=d, years=years, \
            symbol=symbol)
        await write_stats(stats=stats, broker=broker, symbol=symbol, \
            period=period, system=system, direction=d)
    except Exception as e:
        print(colored.red("At stats_process {}".format(e)))


async def loop_over_strats(path_to, filename, loop):
    try:
        if settings.SHOW_DEBUG:
            print("Stats Working with {}".format(look_for))

        info = name_deconstructor(filename=filename, t="s")
        file_name = join(path_to, info["filename"])

        symbol = None
        try:
            symbol = Symbols.objects.get(symbol=info["symbol"])
        except:
            pass
        period = Periods.objects.get(period=info["period"])
        try:
            if settings.SHOW_DEBUG:
                print("System: {}".format(info["system"]))
            system = Systems.objects.get(title=info["system"])
        except:
            system = None

        if (not system is None) & (not symbol is None):
            broker = Brokers.objects.get(title=info["broker"])
            df = await df_multi_reader(filename=file_name)

            try:
                years = df.index[-1].year - df.index[0].year + ((12 - df.index[0].month) + df.index[-1].month) / 12.0
            except Exception as e:
                print(colored.red("At years {}\n".format(e)))
                years = None

            if not years is None:
                df['BUYSELL'] = df['LONG_PL'] + df['SHORT_PL']
                df['BUYSELL_CUMSUM'] = df['BUYSELL'].cumsum()

                directions = [0, 1, 2]
                for d in directions:
                    await stats_process(df=df, d=d, years=years, broker=broker, \
                        symbol=symbol, period=period, system=system)
    except Exception as err:
        print(colored.red("At loop over strats {}\n".format(err)))
        await file_cleaner(filename=file_name)


async def generate_qindexd_stats(broker):
    try:
        if settings.SHOW_DEBUG:
            print("trying to generate Qindex stats")

        filename = join(settings.DATA_PATH, 'portfolios', '{}_qndx'.format(broker.slug))
        df = await df_multi_reader(filename=filename, limit=True)

        years = df.index[-1].year - df.index[0].year + ((12 - df.index[0].month) + df.index[-1].month) / 12.0

        stats = {}
        try:
            symbol = Symbols.objects.get(symbol='AI50')
        except IntegrityError:
            symbol = Symbols.objects.create(symbol='AI50')

        period = Periods.objects.get(period=1440)
        try:
            system = Systems.objects.get(title='AI50')
        except IntegrityError:
            system = Systems.objects.create(title='AI50')
        print(system)

        stats['margin'] = df.LONG_MARGIN.max()
        stats['std'] = await std_func(data=df.LONG_PL)
        stats['var'] = await var_func(data=df.LONG_PL)
        stats['avg_trade'] = await avg_trade_func(data=df.LONG_PL)
        stats['sharpe'] = await sharpe_func(avg_trade=stats['avg_trade'], std=stats['std'])
        stats['sortino'] = await sortino_ratio(returns=df.LONG_PL.as_matrix(), target=0)
        stats['bh_std'] = await std_func(data=df.DIFF)
        stats['bh_avg_trade'] = await avg_trade_func(data=df.DIFF)
        stats['bh_sharpe'] = await sharpe_func(avg_trade=stats['bh_avg_trade'], std=stats['bh_std'])
        stats['bh_sortino'] = await sortino_ratio(returns=df.DIFF.as_matrix(), target=0)
        stats['avg_win'], stats['avg_loss'] = await avg_win_loss_func(data=df.LONG_PL)
        stats['win_rate'] = await win_rate_func(data=df.LONG_PL)
        stats['trades'] = await trades_func(data=df.LONG_TRADES)
        stats['gross_profit'], stats['gross_loss'], stats['total_profit'], \
            stats['fr'] = await fr_func(avg_trade=stats['avg_trade'], \
            trades=stats['trades'], returns=df.LONG_PL, cumsum=df.LONG_PL_CUMSUM)
        stats['max_dd'] = await max_dd_func(data=df.LONG_PL_CUMSUM)
        stats['intraday_dd'] = await idd_func(df=df, direction=1)
        stats['yearly'], stats['acc_minimum'], stats['avg_yearly_p'] = await yearly_func(\
            total_profit=stats['total_profit'], years=years, \
            max_dd=stats['max_dd'], intraday_dd=stats['intraday_dd'], symbol=None, margin=stats['margin'])

        try:
            s = Stats.objects.create(broker=broker, symbol=symbol, period=period, \
                system=system, direction=1, sharpe=stats['sharpe'], \
                std=stats['std'], var=stats['var'], avg_trade=stats['avg_trade'], \
                avg_win=stats['avg_win'], avg_loss=stats['avg_loss'], \
                win_rate=stats['win_rate'], trades=stats['trades'], fitness=stats['fr'], \
                intraday_dd=stats['intraday_dd'], max_dd=stats['max_dd'], \
                total_profit=stats['total_profit'], yearly=stats['yearly'], yearly_p=stats['avg_yearly_p'], \
                acc_minimum=stats['acc_minimum'], bh_sharpe=stats['bh_sharpe'], \
                sortino=stats['sortino'], bh_sortino=stats['bh_sortino'])
            s.save()
            symbol.margin_initial = stats['margin']
            symbol.save()
            print(colored.green("Wrote Qindex stats to db"))
        except IntegrityError:
            await update_stats(broker=broker, symbol=symbol, period=period, system=system, \
                direction=1, stats=stats)
    except Exception as err:
        print(colored.red("At generate_qindexd_stats {}".format(err)))


def generate_stats(loop):
    brokers = Brokers.objects.all()
    path_to = join(settings.DATA_PATH, "performance")
    filenames = multi_filenames(path_to_history=path_to)

    loop.run_until_complete(gather(*[loop_over_strats(\
        path_to=path_to, filename=filename, loop=loop) for filename \
        in filenames], return_exceptions=True))

    loop.run_until_complete(gather(*[generate_qindexd_stats(broker=broker) for broker in brokers],
        return_exceptions=True))