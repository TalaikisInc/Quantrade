from django.contrib import admin
from django.contrib.flatpages.models import FlatPage
from django.contrib.flatpages.admin import FlatPageAdmin
from django.utils.translation import ugettext_lazy as _
from django.conf import settings
from django.core.urlresolvers import reverse
from django import forms
from django.db import models

from ckeditor.widgets import CKEditorWidget

from collector.models import (QtraUser, Portfolios, Symbols, Brokers, Periods,
    Stats, Contacts, Signals, Indicators, Systems, PortfolioData, PortfolioStrats,
    Post, Corr, GARCH, Strategy, Indicator, MCJobs)


class PostAdminForm(forms.ModelForm):
    content = forms.CharField(widget=CKEditorWidget())

    class Meta:
        model = Post
        fields = "__all__"


class PostAdmin(admin.ModelAdmin):
    form = PostAdminForm


class FlatPageCustom(FlatPageAdmin):
    formfield_overrides = {
        models.TextField: {'widget': CKEditorWidget}
    }


class SymbolsAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'sources', 'commission', 'margin_initial', \
        'spread', 'tick_value', 'tick_size', 'digits', 'price', 'currency')
    list_filter = ['broker', 'sources']
    search_fields = ['symbol']


class SignalsAdmin(admin.ModelAdmin):
    list_display = ('date_time', 'symbol', 'returns', 'posted_to_twitter', \
        'posted_to_facebook', 'sent_email')
    list_filter = ['symbol']
    search_fields = ['symbol', 'system', 'broker']


class StatsAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'period', 'system', 'direction')
    list_filter = ['broker', 'period', 'system', 'direction', 'symbol']
    search_fields = ['symbol', 'period', 'system', 'direction']


class GARCHAdmin(admin.ModelAdmin):
    list_display = ('broker', 'symbol', 'date_time', 'period', 'change')
    list_filter = ['broker', 'period', 'symbol']
    search_fields = ['broker', 'period', 'symbol', 'change']


class MCJobsAdmin(admin.ModelAdmin):
    list_display = ('filename', 'status', 'direction')
    list_filter = ['status', 'direction']
    search_fields = ['filename', 'direction']


admin.site.unregister(FlatPage)
admin.site.register(FlatPage, FlatPageCustom)
admin.site.register(QtraUser)
admin.site.register(Corr)
admin.site.register(Portfolios)
admin.site.register(Symbols, SymbolsAdmin)
admin.site.register(Brokers)
admin.site.register(Periods)
admin.site.register(Stats, StatsAdmin)
admin.site.register(Contacts)
admin.site.register(Post, PostAdmin)
admin.site.register(Indicator)
admin.site.register(Strategy)
admin.site.register(GARCH, GARCHAdmin)
admin.site.register(Signals, SignalsAdmin)
admin.site.register(Indicators)
admin.site.register(Systems)
admin.site.register(PortfolioData)
admin.site.register(PortfolioStrats)
admin.site.register(MCJobs, MCJobsAdmin)