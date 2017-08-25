from os.path import join, isfile
from datetime import datetime, date
import inspect
from functools import lru_cache

from django.utils.encoding import force_text
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from django.conf import settings
from django.utils.translation import ugettext as T
from django.http import Http404, HttpResponseRedirect
from django.core.urlresolvers import reverse
from django.template.defaultfilters import slugify

import plotly.offline as opy
import plotly.graph_objs as go
from plotly import figure_factory
from numpy import sum
from pandas import to_datetime

from .models import QtraUser, Portfolios, Periods, Symbols, Systems, Stats,\
    Brokers, Signals, Post, Corr, GARCH
from .tasks import error_email, nonasy_df_multi_reader

systems = Systems.objects.all().order_by('title')
periods = Periods.objects.all()
symbols = Symbols.objects.all().order_by('symbol')
brokers = Brokers.objects.all().order_by('title')


#TODO refactor,has many duplicates

def blog(request):
    posts = Post.objects.filter().order_by('date_time').reverse()
    return render(request, '{}/blog.html'.format(settings.TEMPLATE_NAME), {'posts': posts })


def garch(request, **kwargs):
    try:
        broker_slug = kwargs['broker_slug']
        symbol_slug = kwargs['symbol_slug']
        period_slug = kwargs['period_slug']

        period = Periods.objects.get(period=period_slug)
        broker = Brokers.objects.get(slug=broker_slug)
        symbol = Symbols.objects.get(symbol=symbol_slug)

        img_file ='/static/collector/images/garch/{0}=={1}=={2}.png'.\
            format(broker_slug, symbol_slug, period_slug)

        gval = GARCH.objects.filter(symbol=symbol, period=period, \
            broker=broker).order_by('date_time').reverse()

        return render(request, '{}/garch_page.html'.format(settings.TEMPLATE_NAME), {'img_file': \
            img_file, 'symbol_slug': symbol_slug, 'period_slug': period, 'gval': gval })
    except:
        return HttpResponseRedirect('/')


def get_portfolios(logged_user):
    try:
        portfolios_ = Portfolios.objects.select_related('symbol', 'period', 'system'\
            ).filter(user=logged_user)
        portfolios = portfolios_.values('symbol__symbol', 'period__period', \
            'period__name', 'system__title', 'broker__slug', 'direction', \
            'symbol__broker__title', 'symbol', 'period', 'system', \
            'symbol__margin_initial', 'symbol__broker', 'size', 'id')
        cnt = portfolios_.count()
    except:
        portfolios = None
        cnt = 0

    return (portfolios, cnt, portfolios_)


@lru_cache(maxsize=None)
def get_index_stats(broker_slug, symbol_slug, period_slug, system_slug, direction):
    try:
        stats = Stats.objects.select_related('symbol', 'period', 'system', \
            'broker').filter(symbol__symbol=symbol_slug, period__period=period_slug, \
            system__title=system_slug, direction=direction, broker__slug=broker_slug).values('symbol__symbol', \
            'symbol__currency', 'direction', 'period__name', \
            'period__period', 'system__title', 'broker__slug', \
            'sharpe', 'bh_sharpe', 'avg_trade', 'avg_win', 'avg_loss', 'win_rate', \
            'trades', 'fitness', 'symbol__description', 'symbol__commission', \
            'max_dd', 'intraday_dd', 'symbol__margin_initial', \
            'broker__title', 'acc_minimum', 'total_profit', \
            'yearly', 'yearly_p', 'broker', 'symbol', 'period', 'system', \
            'sortino', 'bh_sortino', 'heatmap', 'img', 'yearly_ret', 'mc')
        stats_ = stats[0]
    except:
        stats = None

    return stats_


def get_index_portfolio(logged_user, stats):
    try:
        portfolio = Portfolios.objects.get(user=logged_user, \
            broker=stats['broker'], symbol=stats['symbol'], \
            period=stats['period'], system=stats['system'], \
            direction=stats['direction'])
    except:
        portfolio = []

    return portfolio


def graph_creation(strategy, data, mae_, system_slug, symbol_slug, period, direction, bh_title):
    try:
        if not (strategy is None):
            strategy_ = strategy.loc[strategy != 0.0]
            x = strategy.loc[strategy != 0].index
            mae_ = mae_.loc[(mae_ != mae_.shift()) | (strategy != 0.0)]

            #graphs
            strategy_graph = go.Scatter(x=x, y=strategy_, mode="lines",  \
                name=T('Strategy'), line=dict(color=('rgb(205, 12, 24)'), width=6))
            mae_graph = go.Scatter(x=x, y=mae_, mode="markers",  \
                name=T('Strategy MAE'), line=dict(color=('rgb(115,115,115,1)'), \
                width=4, dash='dot'))

            if not (data is None):
                data_ = data.loc[strategy != 0.0]
                data_graph = go.Scatter(x=x, y=data_, mode="lines",  name=bh_title, \
                    line=dict(color=('rgb(22, 96, 167)'), width=4, dash='dot'))
                title = "{0} on {1} {2} {3}".format(system_slug, symbol_slug, \
                    period, direction)
            #else:
                #title = "Portfolio {}".format(system_slug)

            if not (data is None):
                fig_data = go.Data([data_graph, strategy_graph, mae_graph], \
                    showgrid=True)
            else:
                fig_data = go.Data([strategy_graph, mae_graph], showgrid=True)

            layout = go.Layout(title=title, xaxis={'title':T('Dates')}, \
                yaxis={'title':T('$')}, font=dict(family='Courier New, \
                monospace', size=14, color='#7f7f7f') )

            figure = go.Figure(data=fig_data, layout=layout)

            graph = opy.plot(figure, auto_open=False, output_type='div')
        else:
            graph = None

        return graph
    except Exception as e:
        #stck = inspect.stack()
        #msg='{0} by {1}: {2}'.format(stck[0][3], stck[1][3], e)
        #error_email(e=msg)
        return None


def _get_stats(indx):
    try:
        stats = Stats.objects.select_related('symbol', 'period', 'broker', \
            'system').filter(sharpe__gt=settings.SHARPE, \
            trades__gt=settings.MIN_TRADES, win_rate__gt=settings.WIN_RATE \
            ).exclude(avg_trade__isnull=True, trades__isnull=True, \
            symbol__commission__isnull=True).order_by('sharpe').reverse().values('symbol__symbol', \
            'symbol__currency', 'direction', 'period__name', \
            'period__period', 'system__title', 'broker__slug', \
            'sharpe', 'bh_sharpe', 'avg_trade', 'avg_win', 'avg_loss', 'win_rate', \
            'trades', 'fitness', 'symbol__description', 'symbol__commission', \
            'max_dd', 'intraday_dd', 'symbol__margin_initial', \
            'broker__title', 'acc_minimum', 'total_profit', \
            'yearly', 'yearly_p', 'broker', 'symbol', 'period', 'system', \
            'sortino', 'bh_sortino', 'heatmap', 'img', 'yearly_ret', 'mc')
        pages = len(stats)
        return (stats[indx], pages)
    except Exception as e:
        #stck = inspect.stack()
        #msg='{0} by {1}: {2}'.format(stck[0][3], stck[1][3], e)
        #error_email(e=msg)
        stats = []
        pages = 0
        return (stats, pages)


def get_weighting_style(w, lots):
    try:
        if (w/lots) == 1.0:
            weighting = 2
        else:
            weighting = 1
    except:
        weighting = 2

    return weighting


def latest_view(request):
    try:
        signals = Signals.objects.select_related('symbol', 'period', 'system'\
            'user').filter().order_by('date_time'\
            ).values('symbol__symbol', 'period__name', 'system__title', \
            'direction', 'date_time', 'returns', 'symbol__broker__title', \
            'period__period', 'broker__slug')

        return render(request, '{}/latest_signals.html'.format(settings.TEMPLATE_NAME), {'signals': signals })
    except Exception as e:
        #stck = inspect.stack()
        #msg='{0} by {1}: {2}'.format(stck[0][3], stck[1][3], e)
        #error_email(e=msg)
        raise Http404


def systems_page(request):
    try:
        stats_ = Stats.objects.select_related('symbol', 'period', 'system', \
            'broker').filter(sharpe__gt=settings.SHARPE, trades__gt=settings.MIN_TRADES, \
            win_rate__gt=settings.WIN_RATE, yearly_p__gte=settings.MIN_YEARLY).exclude(avg_trade__isnull=True, \
            trades__isnull=True).order_by('sharpe').reverse().values('fitness',\
            'symbol__symbol', 'period__name', 'period__period', 'system__title', \
            'sharpe', 'bh_sharpe', 'sortino', 'bh_sortino', 'win_rate', 'avg_trade', 'avg_win', 'avg_loss', 'trades', \
            'direction', 'intraday_dd', 'max_dd', 'symbol__currency', \
            'broker__slug', 'symbol__broker__title', 'yearly_p')

        return render(request, '{}/systems.html'.format(settings.TEMPLATE_NAME), {'systems_list': stats_ })
    except Exception as e:
        #stck = inspect.stack()
        #msg='{0} by {1}: {2}'.format(stck[0][3], stck[1][3], e)
        #error_email(e=msg)
        raise Http404


def symbols_page(request):
    try:
        symbols = Symbols.objects.select_related('broker').filter().exclude(\
            commission__isnull=True\
            ).order_by('symbol')
        return render(request, '{}/symbols.html'.format(settings.TEMPLATE_NAME), {'symbols': symbols, \
            'periods': periods })
    except:
        raise Http404


@login_required
def success_process(request):
    pass


def get_account_status(logged_user):
    try:
        if not (logged_user.account_number is None):
            is_account_number = True
            if logged_user.user_type == 1:
                is_customer = True
            else:
                is_customer = False
        else:
            is_account_number = False
            if logged_user.user_type == 1:
                is_customer = True
            else:
                is_customer = False
    except:
        is_customer = False
        is_account_number = False

    return (is_customer, is_account_number)


def get_direction(stats):
    if stats['direction'] == 1:
        direction_slug = 'longs'
    elif stats['direction'] == 1:
        direction_slug = 'shorts'
    else:
        direction_slug = 'longs_shorts'

    return direction_slug


@lru_cache(maxsize=None)
def get_symbols():
    ignored = ['AI50', 'M1']
    symbols = Symbols.objects.filter().exclude(\
        symbol__in=ignored).order_by('symbol').values(\
        'symbol', 'broker__slug')
    return symbols


def get_indx_data(df, stats):
    try:
        if stats['direction'] == 1:
            strategy = df['LONG_PL_CUMSUM']
            mae_ = df['LONG_MAE']
            direction = 'Longs'
            hist = list(df['LONG_PL'].loc[df['LONG_PL'] != 0])
            data = df['LONG_DIFF_CUMSUM']
            hold_hist = list(df['DIFF'].loc[df['DIFF'] != 0])
            bh_title = T('Buy & hold')
        elif stats['direction'] == 2:
            strategy = df['SHORT_PL_CUMSUM']
            mae_ = df['SHORT_MAE']
            direction = 'Shorts'
            hist = list(df['SHORT_PL'].loc[df['SHORT_PL'] != 0])
            data = df['SHORT_DIFF_CUMSUM']
            hold_hist = list(df['DIFF'].loc[df['DIFF'] != 0])
            bh_title = T('Short & hold')
        elif stats['direction'] == 0:
            strategy = df['LONG_PL_CUMSUM'] + df['SHORT_PL_CUMSUM']
            mae_ = df['SHORT_MAE'] + df['LONG_MAE']
            direction = 'Longs & shorts'
            df['OVERALL'] = df['SHORT_PL'] + df['LONG_PL']
            hist = list(df['OVERALL'].loc[df['OVERALL'] != 0])
            data = df['LONG_DIFF_CUMSUM']
            hold_hist = list(df['DIFF'].loc[df['DIFF'] != 0])
            bh_title = T('Buy & hold')
    except Exception as e:
        strategy = None
        mae_ = None
        direction = None
        hist = None
        data = None
        hold_hist = None
        bh_title = None

    return (strategy, mae_, direction, hist, data, hold_hist, bh_title)


def get_indx_graphs(hist, hold_hist, strategy, data, mae_, system_slug, \
    symbol_slug, period, direction, bh_title):
    try:
        hist_graph = hist_graph_creation(hist=hist, hold_hist=hold_hist)
        graph = graph_creation(strategy=strategy, data=data, mae_=mae_, \
            system_slug=system_slug, symbol_slug=symbol_slug, period=period, \
            direction=direction, bh_title=bh_title)
    except Exception as e:
        #stck = inspect.stack()
        #msg='{0} by {1}: {2}'.format(stck[0][3], stck[1][3], e)
        #error_email(e=msg)
        hist_graph = None
        graph = None

    return (hist_graph, graph)


#TODO this is idiotic function, change the files to slugs instead
@lru_cache(maxsize=None)
def broekr_slug_to_title(broker_slug):
    broker = Brokers.objects.get(slug=broker_slug)
    return broker.title


def df_maker(stats, logged_user):
    symbol_slug = stats['symbol__symbol']
    period_slug = stats['period__period']
    system_slug = stats['system__title']
    broker_slug = stats['broker__slug']
    direction_slug = get_direction(stats=stats)
    broker = stats['broker__title']
    period = stats['period__name']
    symbol = stats['symbol__symbol']
    system = stats['system__title']
    meta_image = stats['img']
    heat_image = stats['heatmap']
    yearly_image = stats['yearly_ret']
    mc_image = stats['mc']
    portfolio = get_index_portfolio(logged_user=logged_user, stats=stats)

    in_file = join(settings.DATA_PATH, "performance", "{0}=={1}=={2}=={3}".format(\
        broekr_slug_to_title(broker_slug=broker_slug), symbol_slug, period_slug, system_slug))
    df = nonasy_df_multi_reader(filename=in_file, limit=settings.LIMIT_ENABLED)
    df.index = to_datetime(df.index).to_pydatetime()

    return (symbol_slug, period_slug, system_slug, broker_slug, direction_slug, \
        broker, period, symbol, system, meta_image, portfolio, df, heat_image, \
        yearly_image, mc_image)


def IndexPage(request, **kwargs):
    try:
        logged_user_ = request.user.id
        if logged_user_:
            logged_user = QtraUser.objects.get(id=logged_user_)
        else:
            logged_user = None
    except:
        logged_user = None

    is_customer, is_account_number = get_account_status(logged_user)
    symbols = get_symbols() #cached
    stats_ = _get_stats(indx=0) #qs are cached by default
    stats = stats_[0]
    pages = stats_[1]
    first_page = True

    symbol_slug, period_slug, system_slug, broker_slug, direction_slug, broker, \
        period, symbol, system, meta_image, portfolio, df, heat_image, yearly_image, mc_image = \
        df_maker(stats=stats, logged_user=logged_user) #cached by lru

    if len(df) > 10:
        strategy, mae_, direction, hist, data, hold_hist, bh_title = get_indx_data(df=df, \
            stats=stats) #not cached, unless indirectly
    else:
        stats_ = _get_stats(indx=1)
        stats = stats_[0]
        symbol_slug, period_slug, system_slug, broker_slug, direction_slug, \
            broker, period, symbol, system, meta_image, portfolio, df, heat_image, yearly_image, mc_image = \
            df_maker(stats=stats, logged_user=logged_user)
        strategy, mae_, direction, hist, data, hold_hist, bh_title = get_indx_data(df=df, \
            stats=stats)

    hist_graph, graph = get_indx_graphs(hist=hist, hold_hist=hold_hist, \
        strategy=strategy, data=data, mae_=mae_, system_slug=system_slug, \
        symbol_slug=symbol_slug, period=period, direction=direction, \
        bh_title=bh_title) #not cached

    return render(request, '{}/index.html'.format(settings.TEMPLATE_NAME), \
        {'graph': graph, 'symbol_slug': symbol_slug, 'period_slug': period_slug, \
        'symbols': symbols, 'system_slug': system_slug, 'systems': systems, \
        'period': period, 'periods': periods, 'brokers': brokers, 'broker_slug': broker_slug, \
        'direction_slug': direction_slug, 'direction': direction, 'indx': True, \
        'pages': pages, 'meta_image': meta_image, 'hist_graph': hist_graph, \
        'stats': stats, 'portfolio': portfolio, 'is_customer': is_customer, \
        'is_account_number': is_account_number, 'first_page': first_page, \
        'heat_image': heat_image, 'yearly_image': yearly_image, 'mc_image': mc_image })


def get_indx_stats(broker_slug, symbol_slug, period_slug, system_slug, direction_slug):
    try:
        if direction_slug == 'longs':
            stats = get_index_stats(broker_slug=broker_slug, symbol_slug=symbol_slug, period_slug=period_slug, system_slug=system_slug, direction=1)
        elif direction_slug == 'shorts':
            stats = get_index_stats(broker_slug=broker_slug, symbol_slug=symbol_slug, period_slug=period_slug, system_slug=system_slug, direction=2)
        else:
            stats = get_index_stats(broker_slug=broker_slug, symbol_slug=symbol_slug, period_slug=period_slug, system_slug=system_slug, direction=0)
    except:
        stats = None

    return stats


def IndexPageRequested(request, **kwargs):
    try:
        logged_user_ = request.user.id
        if logged_user_:
            logged_user = QtraUser.objects.get(id=logged_user_)
        else:
            logged_user = None
    except:
        logged_user = None

    is_customer, is_account_number = get_account_status(logged_user)
    try:
        broker_slug = kwargs['broker_slug']
        symbol_slug = kwargs['symbol_slug']
        period_slug = kwargs['period_slug']
        system_slug = kwargs['system_slug']
        direction_slug = kwargs['direction_slug']
        stats = get_indx_stats(broker_slug, symbol_slug, period_slug, system_slug, direction_slug)
        if not (stats is None):
            meta_image = stats['img']
            heat_image = stats['heatmap']
            yearly_image = stats['yearly_ret']
            mc_image = stats['mc']
            portfolio = get_index_portfolio(logged_user=logged_user, stats=stats)
            broker = stats['broker__title']
            symbol = stats['symbol__symbol']
            period = stats['period__name']
            system = stats['system__title']
    except Exception as e:
        print(e)
        #raise Http404
        return HttpResponseRedirect('/')

    first_page = False
    pages = None
    symbols = get_symbols()
    try:
        in_file = join(settings.DATA_PATH, "performance", "{0}=={1}=={2}=={3}".format(\
            broekr_slug_to_title(broker_slug=broker_slug), symbol_slug, period_slug, system_slug))
        df = nonasy_df_multi_reader(filename=in_file, limit=settings.LIMIT_ENABLED)
    
        strategy, mae_, direction, hist, data, hold_hist, bh_title = get_indx_data(df=df, \
            stats=stats)
        hist_graph, graph = get_indx_graphs(hist=hist, hold_hist=hold_hist, \
            strategy=strategy, data=data, mae_=mae_, system_slug=system_slug, \
            symbol_slug=symbol_slug, period=period, direction=direction, \
            bh_title=bh_title)

        return render(request, '{}/index.html'.format(settings.TEMPLATE_NAME), {'graph': graph,
            'symbol_slug': symbol_slug, 'period_slug': period_slug,
            'symbols': symbols, 'system_slug': system_slug, 'systems': systems, \
            'period': period, 'periods': periods, 'brokers': brokers, \
            'broker_slug': broker_slug, 'direction_slug': direction_slug,
            'direction': direction, 'indx': True, 'meta_image': meta_image, \
            'pages': pages, 'hist_graph': hist_graph, 'stats': stats, \
            'portfolio': portfolio, 'is_customer': is_customer, \
            'is_account_number': is_account_number, 'first_page': first_page,
            'heat_image': heat_image, 'yearly_image': yearly_image, 'mc_image': mc_image })

    except Exception as e:
        if settings.SHOW_DEBUG:
            print(e)
        #raise Http404
        return HttpResponseRedirect('/')


def hist_graph_creation(hist, hold_hist):
    try:
        if len(hist) > 1:
            hist_data = [hist, hold_hist]
            group_labels = [T('Strategy'), T('Buy & hold')]
            hist_fig = figure_factory.create_distplot(hist_data, group_labels, bin_size=100)
            hist_graph = opy.plot(hist_fig, auto_open=False, output_type='div', include_plotlyjs=False)
        else:
            hist_graph = None

        return hist_graph
    except:
        return None


def pages_func(order_slug, pages):
    next_page = order_slug + 1
    if next_page >= 0:
        if next_page <= pages:
            prev_page = order_slug - 1
            this_page = order_slug
        else:
            prev_page = order_slug - 1
            next_page = None
            this_page = order_slug
    else:
        prev_page = 0
        this_page = 1

    return (prev_page, next_page, this_page)


def systems_page_ordered_full(request, **kwargs):
    try:
        order_slug = int(kwargs['order_slug'])
    except:
        raise Http404

    try:
        stats_ = _get_stats(indx=order_slug)
        stats = stats_[0]
        pages = stats_[1]-1
    except:
        #raise Http404
        return HttpResponseRedirect('/')

    prev_page, next_page, this_page = pages_func(order_slug=order_slug, pages=pages)

    try:
        logged_user_ = request.user.id
        if logged_user_:
            logged_user = QtraUser.objects.get(id=logged_user_)
        else:
            logged_user = None
    except:
        logged_user = None

    is_customer, is_account_number = get_account_status(logged_user)
    symbol_slug = stats['symbol__symbol']
    period_slug = stats['period__period']
    system_slug = stats['system__title']
    broker_slug = stats['broker__slug']
    first_page = False
    meta_image = stats['img']
    heat_image = stats['heatmap']
    yearly_image = stats['yearly_ret']
    mc_image = stats['mc']
    broker = stats['broker']
    period = stats['period']
    direction_slug = get_direction(stats=stats)
    symbols = get_symbols()
    in_file = join(settings.DATA_PATH, "performance", "{0}=={1}=={2}=={3}".format(\
        broekr_slug_to_title(broker_slug=broker_slug), symbol_slug, period_slug, system_slug))
    df = nonasy_df_multi_reader(filename=in_file)
    strategy, mae_, direction, hist, data, hold_hist, bh_title = get_indx_data(df=df, \
        stats=stats)
    portfolio = get_index_portfolio(logged_user=logged_user, stats=stats)
    symbol = stats['symbol__symbol']
    period = stats['period__name']
    system = stats['system__title']
    hist_graph, graph = get_indx_graphs(hist=hist, hold_hist=hold_hist, \
        strategy=strategy, data=data, mae_=mae_, system_slug=system_slug, \
        symbol_slug=symbol_slug, period=period, direction=direction, \
        bh_title=bh_title)

    return render(request, '{}/systems_ordered.html'.format(settings.TEMPLATE_NAME), {'graph': graph,
        'symbol_slug': symbol_slug, 'period_slug': period_slug, 'symbols': symbols,
        'system_slug': system_slug, 'systems': systems, 'period': period,
        'periods': periods, 'brokers': brokers, 'next_page': next_page,
        'prev_page': prev_page, 'broker_slug': broker_slug, 'pages': pages,
        'meta_image': meta_image, 'heat_image': heat_image, 'this_page': this_page,
        'direction': direction, 'direction_slug': direction_slug, 'hist_graph': hist_graph,
        'stats': stats, 'portfolio': portfolio, 'is_customer': is_customer,
        'is_account_number': is_account_number, 'first_page': first_page,
        'full_v': True, 'yearly_image': yearly_image, 'ordered': True, 'mc_image': mc_image })


def systems_page_ordered(request, **kwargs):
    try:
        order_slug = int(kwargs['order_slug'])
    except:
        raise Http404

    try:
        stats_ = _get_stats(indx=order_slug)
        stats = stats_[0]
        pages = stats_[1]-1
    except:
        #raise Http404
        return HttpResponseRedirect('/')

    prev_page, next_page, this_page = pages_func(order_slug=order_slug, pages=pages)

    try:
        logged_user_ = request.user.id
        if logged_user_:
            logged_user = QtraUser.objects.get(id=logged_user_)
        else:
            logged_user = None
    except:
        logged_user = None

    is_customer, is_account_number = get_account_status(logged_user)
    symbol_slug = stats['symbol__symbol']
    period_slug = stats['period__period']
    system_slug = stats['system__title']
    broker_slug = stats['broker__slug']
    first_page = False
    meta_image = stats['img']
    heat_image = stats['heatmap']
    yearly_image = stats['yearly_ret']
    mc_image = stats['mc']
    broker = stats['broker']
    period = stats['period']
    direction_slug = get_direction(stats=stats)
    symbols = get_symbols()
    in_file = join(settings.DATA_PATH, "performance", "{0}=={1}=={2}=={3}".format(\
        broekr_slug_to_title(broker_slug=broker_slug), symbol_slug, period_slug, system_slug))
    df = nonasy_df_multi_reader(filename=in_file, limit=settings.LIMIT_ENABLED)
    strategy, mae_, direction, hist, data, hold_hist, bh_title = get_indx_data(df=df, \
        stats=stats)
    portfolio = get_index_portfolio(logged_user=logged_user, stats=stats)
    symbol = stats['symbol__symbol']
    period = stats['period__name']
    system = stats['system__title']
    hist_graph, graph = get_indx_graphs(hist=hist, hold_hist=hold_hist, \
        strategy=strategy, data=data, mae_=mae_, system_slug=system_slug, \
        symbol_slug=symbol_slug, period=period, direction=direction, \
        bh_title=bh_title)

    return render(request, '{}/systems_ordered.html'.format(settings.TEMPLATE_NAME), {'graph': graph,
        'symbol_slug': symbol_slug, 'period_slug': period_slug, 'symbols': symbols,
        'system_slug': system_slug, 'systems': systems, 'period': period,
        'periods': periods, 'brokers': brokers, 'next_page': next_page,
        'prev_page': prev_page, 'broker_slug': broker_slug, 'pages': pages,
        'meta_image': meta_image, 'heat_image': heat_image, 'this_page': this_page,
        'direction': direction, 'direction_slug': direction_slug, 'hist_graph': hist_graph,
        'stats': stats, 'portfolio': portfolio, 'is_customer': is_customer,
        'is_account_number': is_account_number, 'first_page': first_page,
        'yearly_image': yearly_image, 'ordered': True, 'full_v': False, 'mc_image': mc_image })


@login_required
def my_performance(request):
    try:
        try:
            logged_user_ = request.user.id
            logged_user = QtraUser.objects.get(id=logged_user_)
        except:
            logged_user = None

        try:
            signals = Signals.objects.select_related('symbol', 'period', 'system'\
                'user').filter().order_by('date_time'\
                ).reverse().values('symbol__symbol', 'period__name', 'system__title', \
                'direction', 'date_time', 'returns', 'symbol__broker__title', \
                'period__period', 'broker__slug')
        except:
            signals = None

        return render(request, '{}/signals.html'.format(settings.TEMPLATE_NAME), {'signals': signals, 'logged_user': logged_user})
    except Exception as e:
        stck = inspect.stack()
        msg='{0} by {1}: {2}'.format(stck[0][3], stck[1][3], e)
        error_email(e=msg)
        raise Http404


def api_main(request):
    return render(request, '{}/api.html'.format(settings.TEMPLATE_NAME))


def auto_portfolio(request):
    return render(request, '{}/auto_portfolio_list.html'.format(settings.TEMPLATE_NAME), {'brokers': brokers })


def auto_portfolio_page(request, **kwargs):
    try:
        broker_slug = kwargs['broker_slug']
        broker_slug = force_text(broker_slug, encoding='utf-8', strings_only=True, errors='strict')
    except:
        return HttpResponseRedirect('/')

    broker = Brokers.objects.get(slug=broker_slug)
    filename = join(settings.DATA_PATH, 'portfolios', '{}_qndx'.format(broker.slug))
    df = nonasy_df_multi_reader(filename=filename, limit=True)

    hist = list(df['LONG_PL'].loc[df['LONG_PL'] != 0])
    hold_hist = list(df['DIFF'].loc[df['DIFF'] != 0])

    stats = Stats.objects.get(symbol__symbol='AI50', period__period='1440', \
        system__title='AI50', direction=1, broker=broker)

    hist_graph = hist_graph_creation(hist=hist, hold_hist=hold_hist)

    margin_data = go.Scatter(x=df.index, y=df.LONG_MARGIN, mode="lines",  \
        name=T('Margin'), line=dict(color=('rgb(205, 12, 24)'), width=2))
    trades_data = go.Scatter(x=df.index, y=df.LONG_TRADES, mode="lines",  \
        name=T('Trades'), line=dict(color=('rgb(205, 12, 24)'), width=2))
    pl_data = go.Scatter(x=df.index, y=df.LONG_PL_CUMSUM, mode="lines",  \
        name=T('Cumulative returns'), line=dict(color=('rgb(205, 12, 24)'), width=6))
    mae_data = go.Scatter(x=df.index, y=df.LONG_MAE, mode="markers",  \
        name=T('MAE'), line=dict(color=('rgb(115,115,115,1)'), width=4, dash='dot'))

    margin_g = go.Data([margin_data], showgrid=True)
    trades_g = go.Data([trades_data], showgrid=True)
    main_g = go.Data([pl_data, mae_data], showgrid=True)

    main_layout = go.Layout(title='Autoportfolio Index 50 performance', xaxis={'title':T('Dates')}, \
        yaxis={'title':T('$')}, font=dict(family='Courier New, \
        monospace', size=14, color='#7f7f7f') )
    margin_layout = go.Layout(title='Used margin over time', xaxis={'title':T('Dates')}, \
        yaxis={'title':T('$')}, font=dict(family='Courier New, \
        monospace', size=14, color='#7f7f7f') )
    trades_layout = go.Layout(title='Trades over time', xaxis={'title':T('Dates')}, \
        yaxis={'title':T('Count')}, font=dict(family='Courier New, \
        monospace', size=14, color='#7f7f7f') )

    margin_fig = go.Figure(data=margin_g, layout=margin_layout)
    trades_fig = go.Figure(data=trades_g, layout=trades_layout)
    main_fig = go.Figure(data=main_g, layout=main_layout)

    margin_graph = opy.plot(margin_fig, auto_open=False, output_type='div')
    trades_graph = opy.plot(trades_fig, auto_open=False, output_type='div')
    graph = opy.plot(main_fig, auto_open=False, output_type='div')

    return render(request, '{}/auto_portfolio.html'.format(settings.TEMPLATE_NAME), {
            'graph': graph, 'hist_graph': hist_graph, 'stats': stats,
            'autoportfolio': True, 'heatmap': stats.heatmap, 'margin_graph': margin_graph,
            'trades_graph': trades_graph, 'yearly_img': stats.yearly_ret, 'broker': broker
         })


def corr_main(request):
    return render(request, '{}/corr_main.html'.format(settings.TEMPLATE_NAME), {'symbols': symbols })


def corr_page(request, **kwargs):
    try:
        symbol_slug = kwargs['symbol_slug']
        symbol_slug = force_text(symbol_slug, encoding='utf-8', strings_only=True, errors='strict')
    except:
        return HttpResponseRedirect('/')

    symbol_a = Symbols.objects.get(symbol=symbol_slug)
    corr = Corr.objects.filter(symbol_a=symbol_a).order_by('value')
    #, value__lte=-0.25, value__gte=0.25

    return render(request, '{}/corr_page.html'.format(settings.TEMPLATE_NAME), {'corrs': corr, 'symbol_slug': symbol_slug })
