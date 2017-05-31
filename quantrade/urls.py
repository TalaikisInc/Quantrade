from django.conf.urls import url, include
from django.contrib import admin
from django.conf import settings
from django.conf.urls.static import static
from django.conf.urls import (handler400, handler403, handler404, handler500)
from django.contrib.auth import views as auth_views
from django.contrib.flatpages import views as flat_views

from collector import (views, mongo_views)
from collector.feed import (LatestSignalsFeed, NewsFeed)


urlpatterns = [
    url(r'^admin/', admin.site.urls),

    url(r'^oauth/', include('social.apps.django_app.urls', namespace='social')),

    #translations
    url(r'^rosetta/', include('rosetta.urls')),

    url(r'^$', mongo_views.IndexPage, name='indx_view'),

    url(r'^(?P<broker_slug>[-\w]+)/(?P<symbol_slug>[-\w]+)/(?P<period_slug>[-\w]+)/(?P<system_slug>[-\w]+)/(?P<direction_slug>[-\w]+)/$', mongo_views.IndexPageRequested, name='indx_view_many'),
    #url(r'^delete/(?P<broker_slug>[-\w]+)/(?P<symbol_slug>[-\w]+)/(?P<period_slug>[-\w]+)/(?P<system_slug>[-\w]+)/(?P<direction_slug>[-\w]+)/$', mongo_views.delete_portfolio, name='delete_portfolio'),
    #url(r'^save/(?P<broker_slug>[-\w]+)/(?P<symbol_slug>[-\w]+)/(?P<period_slug>[-\w]+)/(?P<system_slug>[-\w]+)/(?P<direction_slug>[-\w]+)/$', mongo_views.save_portfolio, name='saveportfolio'),

    #python-scial-auth
    url(r'^login/', views.login, name='login'),
    url(r'^logout/', views.logout, name='logout'),
    url(r'^members/$', views.account, name='settings'),
    url(r'^delete_account/(?P<user_slug>[-\w]+)/(?P<confirmed>[-\w]+)$', views.delete_account, name='delete_account'),
    url(r'^password/$', views.password, name='password'),
    url(r'^broker/$', views.broker_page, name='broker'),
    url(r'^email/$', views.email_page, name='email'),
    url(r'^feed/$', LatestSignalsFeed()),
    url(r'^news_feed/$', NewsFeed()),
    url(r'^performance/$', mongo_views.my_performance, name='my_performance'),
    #url(r'^portfolio/$', mongo_views.portfolio_view, name='portfolio'),
    url(r'^api/$', mongo_views.api_main, name='api_main'),
    url(r'^correlations/$', mongo_views.corr_main, name='corr_main'),
    url(r'^correlations/(?P<symbol_slug>[-\w]+)/$', mongo_views.corr_page, name='corr_page'),

    url(r'^blog/$', mongo_views.blog, name='blog'),
    url(r'^garch/(?P<broker_slug>[-\w]+)/(?P<symbol_slug>[-\w]+)/(?P<period_slug>[-\w]+)/$', mongo_views.garch, name='garch'),
    #url(r'^equal_weights/$', mongo_views.EqualWeight, name='equal_weights_portfolio'),
    #url(r'^minimum_variance/$', mongo_views.MinVariance, name='minimum_variance_portfolio'),
    #url(r'^portfolios/$', mongo_views.portfolios_view, name='portfolios'),
    url(r'^login_error/', views.login_error, name='login_error'),

    #contact
    url(r'^contact/$', views.contact, name='contact'),
    url(r'^thanks/$', views.thanks, name='thanks'),
    url(r'^registered/$', views.registered, name='registered'),
    url(r'^form_error/$', views.form_error, name='form_error'),

    #various single dynamic pages
    url(r'^systems/$', mongo_views.systems_page, name='systems_page'),
    url(r'^auto_portfolio/$', mongo_views.auto_portfolio, name='auto_portfolio'),
    url(r'^ai/(?P<broker_slug>[-\w]+)/$', mongo_views.auto_portfolio_page, name='auto_portfolio_page'),
    url(r'^best_systems/(?P<order_slug>[-\w]+)/$', mongo_views.systems_page_ordered, name='systems_page_ordered'),
    url(r'^best_systems/full_history/(?P<order_slug>[-\w]+)/$', mongo_views.systems_page_ordered_full, name='systems_page_full'),
    #url(r'^machine_portfolio/(?P<broker_slug>[-\w]+)/(?P<portfolio_slug>[-\w]+)/$', mongo_views.portfolios_graph_view, name='portfolios_graph'),
    url(r'^latest_signals/$', mongo_views.latest_view, name='latest'),

    #url(r'^indicators/(?P<broker_slug>[-\w]+)/$', mongo_views.indicators_page, name='indicators_page'),
    #url(r'^indicators/(?P<broker_slug>[-\w]+)/(?P<indicator_slug>[-\w]+)/$', mongo_views.indicators_by_symbols_intermediate, name='indicators_page_symbols_i'),
    #url(r'^indicators/i/(?P<broker_slug>[-\w]+)/(?P<symbol_slug>[-\w]+)/(?P<period_slug>[-\w]+)/(?P<indicator_slug>[-\w]+)/$', mongo_views.indicators_by_symbols, name='indicators_page_symbols'),
    url(r'^symbols/$', mongo_views.symbols_page, name='symbols_page'),

    #static pages
    url(r'^privacy_policy/$', flat_views.flatpage, {'url': '/privacy_policy/'}, name='privacy_policy'),
    url(r'^risk_warning/$', flat_views.flatpage, {'url': '/risk_warning/'}, name='risk_warning'),
    url(r'^terms_of_service/$', flat_views.flatpage, {'url': '/terms_of_service/'}, name='terms_of_service'),
    url(r'^about/$', flat_views.flatpage, {'url': '/about/'}, name='about'),
    url(r'^how_it_works/$', flat_views.flatpage, {'url': '/how_it_works/'}, name='how_it_works'),
    url(r'^brokers/$', flat_views.flatpage, {'url': '/brokers/'}, name='brokers'),
    url(r'^trading_advice/$', flat_views.flatpage, {'url': '/trading_advice/'}, name='trading_advice'),

] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT) + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

handler400 = 'collector.views.bad_request'
handler403 = 'collector.views.permission_denied'
handler404 = 'collector.views.page_not_found'
handler500 = 'collector.views.server_error'
