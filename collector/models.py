from django.db import models
from django.utils.translation import ugettext as T
from django.conf import settings
from django.contrib.auth.models import AbstractUser
from django.template.defaultfilters import slugify
from django.contrib.postgres.fields import JSONField, ArrayField


DEVICES = (
    (0, 'None registered'),
    (1, 'iPhone'),
    (2, 'Android'),
    (3, 'Browser'),
)

USER_TYPE = (
    (0, 'User'),
    (1, 'Customer')
)


class AutoSlugifyOnSaveModel(models.Model):
    def save(self, *args, **kwargs):
        pk_field_name = self._meta.pk.name
        value_field_name = getattr(self, 'value_field_name', 'title')
        slug_field_name = getattr(self, 'slug_field_name', 'slug')
        max_interations = getattr(self, 'slug_max_iterations', 1000)
        slug_separator = getattr(self, 'slug_separator', '-')

        # fields, query set, other setup variables
        slug_field = self._meta.get_field(slug_field_name)
        slug_len = slug_field.max_length
        queryset = self.__class__.objects.all()

        # if the pk of the record is set, exclude it from the slug search
        current_pk = getattr(self, pk_field_name)

        if current_pk:
            queryset = queryset.exclude(**{pk_field_name: current_pk})

        # setup the original slug, and make sure it is within the allowed length
        slug = slugify(getattr(self, value_field_name)).replace('-', '_')

        if slug_len:
            slug = slug[:slug_len]

        original_slug = slug

        # iterate until a unique slug is found, or max_iterations
        counter = 2
        while queryset.filter(**{slug_field_name: slug}).count() > 0 and counter < max_interations:
            slug = original_slug
            suffix = '%s%s' % (slug_separator, counter)
            if slug_len and len(slug) + len(suffix) > slug_len:
                slug = slug[:slug_len-len(suffix)]
            slug = '%s%s' % (slug, suffix)
            counter += 1

        if counter == max_interations:
            raise IntegrityError('Unable to locate unique slug')

        setattr(self, slug_field.attname, slug)

        super(AutoSlugifyOnSaveModel, self).save(*args, **kwargs)

    class Meta:
        abstract = True


class Brokers(AutoSlugifyOnSaveModel):
    id = models.BigAutoField(primary_key=True)
    title = models.CharField(max_length=80, verbose_name=T("Broker"), unique=True)
    description = models.TextField(blank=True, null=True, verbose_name=T("Broker description"))
    slug = models.CharField(max_length=80, verbose_name=T("Broker slug"), blank=True, null=True)
    registration_url = models.URLField(verbose_name=T("Registration URL"), null=True, blank=True)
    affiliate_url = models.URLField(verbose_name=T("Affiliate URL"), null=True, blank=True)

    def __unicode__(self):
        return '%s' %(self.title)

    def __str__(self):
        return '%s' %(self.title)


class QtraUser(AbstractUser):
    user_type = models.SmallIntegerField(null=True, blank=True, choices=USER_TYPE, default=0)
    email = models.EmailField(max_length=150, verbose_name=T("Email for signal notifications"))
    userprofile = models.TextField(blank=True)
    avatar = models.ImageField(blank=True, null=True, verbose_name=T("Avatar"))
    g_display_name = models.CharField(max_length=25, null=True, blank=True)
    gender = models.CharField(max_length=15, null=True, blank=True)
    age_range = models.CharField(max_length=35, null=True, blank=True)
    skills = models.CharField(max_length=250, null=True, blank=True)
    twitter_followers = models.IntegerField(verbose_name=T("Twitter followers"), blank=True, null=True)
    gplus_url = models.URLField(verbose_name=T("URL to G+ profile"), null=True, blank=True)
    occupation = models.CharField(max_length=250, null=True, blank=True)
    location = models.CharField(max_length=30, null=True, blank=True)
    birth_date = models.DateField(null=True, blank=True)
    device = models.SmallIntegerField(null=True, blank=True, choices=DEVICES, default=0)
    device_id = models.CharField(max_length=250, null=True, blank=True)
    broker = models.ForeignKey(Brokers, verbose_name=T("Broker"), null=True, blank=True)
    account_number = models.IntegerField(null=True, blank=True, verbose_name=T("Metatrader 4 number"))
    key = models.CharField(max_length=35, null=True, blank=True)
    previous_account_number = None


class Post(models.Model):
    id = models.BigAutoField(primary_key=True)
    title = models.CharField(max_length=150, verbose_name=T("Title"))
    date_time = models.DateTimeField(verbose_name=T("Date"))
    content = models.TextField(verbose_name=T("Content"))

    def __unicode__(self):
        return '%s' %(self.title)

    def __str__(self):
        return '%s' %(self.title)


class Indicator(models.Model):
    id = models.BigAutoField(primary_key=True)
    title = models.CharField(max_length=15, verbose_name=T("Indicator title"), unique=True)
    content = models.TextField(verbose_name=T("Indicator code"))

    def __unicode__(self):
        return '%s' %(self.title)

    def __str__(self):
        return '%s' %(self.title)


class Strategy(models.Model):
    id = models.BigAutoField(primary_key=True)
    title = models.CharField(max_length=15, verbose_name=T("Strategy title"), unique=True)
    content = models.TextField(verbose_name=T("Strategy code"))

    def __unicode__(self):
        return '%s' %(self.title)

    def __str__(self):
        return '%s' %(self.title)


class Contacts(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=120, verbose_name=T("Name"))
    email = models.EmailField(max_length=100, verbose_name=T("Email"))
    user = models.ForeignKey(QtraUser, verbose_name=T("User"))
    subject = models.CharField(max_length=120, verbose_name=T("Subject"))
    message = models.TextField(verbose_name=T("Message"))

    def __unicode__(self):
        return '(%s) %s %s' %(self.name, self.subject, self.message)

    def __str__(self):
        return '(%s) %s %s' %(self.name, self.subject, self.message)


class Periods(models.Model):
    id = models.BigAutoField(primary_key=True)
    period = models.IntegerField(verbose_name=T("Period in minutes"), unique=True)
    name = models.CharField(max_length=4)

    def __unicode__(self):
        return '%s' %(self.name)

    def __str__(self):
        return '%s' %(self.name)


class Symbols(models.Model):
    DATA_SOURCES = (
        (2, 'Metatrader 4'),
        (3, 'Quandl'),
        (5, 'Barchart'),
    )

    id = models.BigAutoField(primary_key=True)
    symbol = models.CharField(max_length=20, db_index=True, verbose_name=T("Instrument symbol"), unique=True)
    description = models.CharField(max_length=50, verbose_name=T("Instrument description"), blank=True, null=True)
    currency = models.CharField(max_length=5, verbose_name=T("Currency"))
    sources = models.SmallIntegerField(verbose_name=T("Data source"), choices=DATA_SOURCES, default=2)
    digits = models.SmallIntegerField(verbose_name=T("Digits"), blank=True, null=True)
    profit_type = models.SmallIntegerField(verbose_name=T("Type of profit clculation"), blank=True, null=True)
    spread = models.DecimalField(max_digits=20, decimal_places=5, verbose_name=T("Spread"), blank=True, null=True)
    tick_value = models.DecimalField(max_digits=20, decimal_places=5, verbose_name=T("Tick value"), blank=True, null=True)
    tick_size = models.DecimalField(max_digits=20, decimal_places=5, verbose_name=T("Tick size"), blank=True, null=True)
    price = models.DecimalField(max_digits=20, decimal_places=5, verbose_name=T("Price at calculation time"), blank=True, null=True)
    commission = models.DecimalField(max_digits=20, decimal_places=5, verbose_name=T("Commission"), blank=True, null=True)
    margin_initial = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Initial margin"), blank=True, null=True)
    broker = models.ForeignKey(Brokers, verbose_name=T("Broker"))

    def __unicode__(self):
        return '%s' %(self.symbol)

    def __str__(self):
        return '%s' %(self.symbol)


"""
class Subscribers(models.Model):
    name = models.CharField(max_length=140, verbose_name=T("Name"), blank=True, null=True)
    email = models.EmailField(max_length=150, verbose_name=T("Email for news"))
    user = models.ForeignKey(QtraUser, verbose_name=T("User"), blank=True, null=True)
    subscribed = models.BooleanField(default=True)
    when = models.DateTimeField(verbose_name=T("Subscribed"), blank=True, null=True)

    , db_index=True, unique=True
    description = models.TextField(blank=True, null=True, verbose_name=T("System description"))
    slug = models.CharField(max_length=20, verbose_name=T("Indicator slug"), blank=True, null=True)

    def __unicode__(self):
        return '%s' %(self.email)

    def __str__(self):
        return '%s' %(self.email)
"""


class Indicators(models.Model):
    id = models.BigAutoField(primary_key=True)
    title = models.CharField(max_length=20, verbose_name=T("Indicator"),db_index=True, unique=True)
    description = models.TextField(blank=True, null=True, verbose_name=T("System description"))
    slug = models.CharField(max_length=20, verbose_name=T("Indicator slug"), blank=True, null=True)
    content = models.TextField(verbose_name=T("Indicator code"), blank=True, null=True)

    def __unicode__(self):
        return '%s' %(self.title)

    def __str__(self):
        return '%s' %(self.title)


class Systems(AutoSlugifyOnSaveModel):
    id = models.BigAutoField(primary_key=True)
    title = models.CharField(max_length=10, verbose_name=T("System"), db_index=True, unique=True)
    description = models.TextField(blank=True, null=True, verbose_name=T("System description"))
    slug = models.CharField(max_length=10, verbose_name=T("Strategy slug"), blank=True, null=True)
    indicator = models.ForeignKey(Indicators, verbose_name=T("System indicator"), blank=True, null=True)

    def __unicode__(self):
        return '%s' %(self.title)

    def __str__(self):
        return '%s' %(self.title)


class GARCH(models.Model):
    id = models.BigAutoField(primary_key=True)
    broker = models.ForeignKey(Brokers, verbose_name=T("Broker"))
    symbol = models.ForeignKey(Symbols, verbose_name=T("Symbol"))
    period = models.ForeignKey(Periods, verbose_name=T("Period"))
    date_time = models.DateTimeField(verbose_name=T("Date"))
    change = models.DecimalField(max_digits=20, decimal_places=6, verbose_name=T("GARCH change"), blank=True, null=True)

    def __unicode__(self):
        return '%s' %(self.symbol)

    def __str__(self):
        return '%s' %(self.symbol)

    class Meta:
        unique_together = (("broker", "symbol", "period", "date_time"),)
        index_together = [["broker", "symbol", "period", "date_time"],]


class Signals(models.Model):
    id = models.BigAutoField(primary_key=True)
    broker = models.ForeignKey(Brokers, verbose_name=T("Broker"))
    symbol = models.ForeignKey(Symbols, verbose_name=T("Symbol"))
    system = models.ForeignKey(Systems, verbose_name=T("System"))
    period = models.ForeignKey(Periods, verbose_name=T("Period"))
    direction = models.SmallIntegerField(choices=settings.DIRECTIONS, default=0, verbose_name=T("Direction"))
    date_time = models.DateTimeField(verbose_name=T("Date"))
    returns = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Return"), default=None, blank=True, null=True)
    posted_to_twitter = models.BooleanField(default=False)
    posted_to_facebook = models.BooleanField(default=False)
    sent_email = models.BooleanField(default=False)

    def __unicode__(self):
        return '%s' %(self.date_time)

    def __str__(self):
        return '%s' %(self.date_time)

    class Meta:
        unique_together = (("broker", "symbol", "period", "system", "direction", "date_time"),)


class Corr(models.Model):
    symbol_a = models.ForeignKey(Symbols, verbose_name=T("Symbol A"), related_name='symbol1')
    symbol_b = models.ForeignKey(Symbols, verbose_name=T("Symbol B"), related_name='symbol2')
    value = models.FloatField(default=None)

    def __unicode__(self):
        return '%s' %(self.value)

    def __str__(self):
        return '%s' %(self.value)

    class Meta:
        unique_together = (("symbol_a", "symbol_b"),)
        index_together = [["symbol_a", "symbol_b"],]


class Portfolios(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(QtraUser, verbose_name=T("User"))
    broker = models.ForeignKey(Brokers, verbose_name=T("Broker"))
    symbol = models.ForeignKey(Symbols, verbose_name=T("Symbol"))
    system = models.ForeignKey(Systems, verbose_name=T("System"))
    period = models.ForeignKey(Periods, verbose_name=T("Period"))
    direction = models.SmallIntegerField(choices=settings.DIRECTIONS, default=0, verbose_name=T("Direction"))
    size = models.DecimalField(max_digits=6, decimal_places=2, verbose_name=T("Strategy size"), default=1.0)

    def __unicode__(self):
        return '%s %s %s %s %s' %(self.user, self.symbol, self.system, self.period, self.direction)

    def __str__(self):
        return '%s %s %s %s %s' %(self.user, self.symbol, self.system, self.period, self.direction)

    class Meta:
        unique_together = ("user", "broker", "symbol", "period", "system", "direction")


class PortfolioStrats(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(QtraUser, verbose_name=T("User"))
    strategy = models.OneToOneField(Portfolios, verbose_name=T("Strategy"))

    def __unicode__(self):
        return '%s %s' %(self.id, self.user)

    def __str__(self):
        return '%s %s' %(self.id, self.user)


class PortfolioData(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.OneToOneField(QtraUser, verbose_name=T("User"))
    margin = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Margin"), blank=True, null=True)
    intraday_dd = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Intrabar drawdown"), blank=True, null=True)
    max_dd = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Max drawdown"), blank=True, null=True)
    sharpe = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Sharpe ratio"), blank=True, null=True)
    sortino = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Sortino ratio"), blank=True, null=True)
    avg_trade = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Average trade"), blank=True, null=True)
    avg_win = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Average win"), blank=True, null=True)
    avg_loss = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Average loss"), blank=True, null=True)
    win_rate = models.DecimalField(max_digits=6, decimal_places=2, verbose_name=T("Win rate"), blank=True, null=True)
    trades = models.IntegerField(verbose_name=T("Trades"), blank=True, null=True)
    fitness = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Fitness function"), blank=True, null=True)
    total_profit = models.DecimalField(max_digits=30, decimal_places=2, verbose_name=T("Total profit"), blank=True, null=True)
    acc_minimum = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Abs. account minimum"), blank=True, null=True)
    yearly = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Yearly"), blank=True, null=True)
    yearly_p = models.DecimalField(max_digits=6, decimal_places=2, verbose_name=T("Yearly percent"), blank=True, null=True)
    lots = models.SmallIntegerField(default=0, verbose_name=T("Number of strategies"))
    needs_renew = models.BooleanField(default=True)
    todel = models.CharField(max_length=200, verbose_name=T("Strategy to delete"), null=True, blank=True)

    def __unicode__(self):
        return '%s %s' %(self.id, self.user)

    def __str__(self):
        return '%s %s' %(self.id, self.user)


class Stats(models.Model):
    id = models.BigAutoField(primary_key=True)
    broker = models.ForeignKey(Brokers, on_delete=models.CASCADE, verbose_name=T("Broker"))
    symbol = models.ForeignKey(Symbols, on_delete=models.CASCADE, verbose_name=T("Symbol"))
    period = models.ForeignKey(Periods, verbose_name=T("Period"))
    system = models.ForeignKey(Systems, verbose_name=T("System"))
    direction = models.SmallIntegerField(choices=settings.DIRECTIONS, default=0, verbose_name=T("Direction"))
    intraday_dd = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Intrabar drawdown"), blank=True, null=True)
    max_dd = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Max drawdown"), blank=True, null=True)
    sharpe = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Sharpe ratio"), blank=True, null=True)
    bh_sharpe = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Buy and hold Sharpe ratio"), blank=True, null=True)
    sortino = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Sortino ratio"), blank=True, null=True)
    bh_sortino = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Buy and hold Sortino ratio"), blank=True, null=True)
    std = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Std"), blank=True, null=True)
    var = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Variance"), blank=True, null=True)
    avg_trade = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Average trade"), blank=True, null=True)
    avg_win = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Average win"), blank=True, null=True)
    avg_loss = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Average loss"), blank=True, null=True)
    win_rate = models.DecimalField(max_digits=10, decimal_places=2, verbose_name=T("Win rate"), blank=True, null=True)
    trades = models.IntegerField(verbose_name=T("Trades"), blank=True, null=True)
    fitness = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Fitness function"), blank=True, null=True)
    total_profit = models.DecimalField(max_digits=30, decimal_places=2, verbose_name=T("Total profit"), blank=True, null=True)
    acc_minimum = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Abs. account minimum"), blank=True, null=True)
    yearly = models.DecimalField(max_digits=20, decimal_places=2, verbose_name=T("Yearly"), blank=True, null=True)
    yearly_p = models.DecimalField(max_digits=6, decimal_places=2, verbose_name=T("Yearly percent"), blank=True, null=True)
    strategy_url = models.CharField(max_length=250, verbose_name=T("Strategy URL"), default="")
    heatmap = models.CharField(max_length=250, verbose_name=T("Heatmap URL"), default="")
    img = models.CharField(max_length=250, verbose_name=T("Strategy image URL"), default="")
    yearly_ret = models.CharField(max_length=250, verbose_name=T("Yearly returns image"), default="")
    mc = models.CharField(max_length=250, verbose_name=T("Monte Carlo image"), default="")

    def __unicode__(self):
        return '%s' %(self.symbol)

    def __str__(self):
        return '%s' %(self.symbol)

    class Meta:
        unique_together = ["broker", "symbol", "period", "system", "direction"]
        index_together = ["broker", "symbol", "period", "system", "direction"]
