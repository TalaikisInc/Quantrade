from django.conf import settings


broker_base = settings.DEFAULT_BROKER
site_name = settings.SITE_NAME

def extra_context(request):
    try:
        user = request.user
    except:
        user = None

    return {'base_url': settings.BASE_URL[:-1],
            'sitename': site_name,
            'user': user,
            'members_area_nae': 'members',
            'broker_base': broker_base,
            'dev': settings.DEV_ENV,
            'version': settings.VERSION,
            'noncustomerlimit': settings.LIMIT_STRATEGIES_FOR_NON_CUSTOMERS }
