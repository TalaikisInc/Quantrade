from time import sleep

from django.db.models.signals import post_save
from django.dispatch import receiver
from django.core.mail import send_mail
from django.conf import settings

from .models import (QtraUser, Portfolios)#, Subscribers, Post)


@receiver(post_save, sender=QtraUser)
def send_new_profile_email(sender, instance, **kwargs):
    try:
        if kwargs["created"]:
            send_mail('New user registered at Quantrade', \
                'Quantrade has new user {}.\n\n'.format(instance), \
                settings.DEFAULT_FROM_EMAIL,
                settings.NOTIFICATIONS_EMAILS, \
                fail_silently=False)

            email = instance.email
            if email:
                send_mail('You had registered at Quantrade!', \
                    'You are now the member of Quantrade quantitative trading signals where you can enjoy hundreds of profitable trading strategies for free!.\n\n', \
                    settings.DEFAULT_FROM_EMAIL,
                    [email], \
                    fail_silently=False)

    except:
        pass


@receiver(post_save, sender=QtraUser)
def remember_account_number(sender, instance, **kwargs):
    instance.previous_account_number = instance.account_number


@receiver(post_save, sender=QtraUser)
def send_email_on_acc_number_update(sender, instance, **kwargs):
    try:
        created = kwargs.get('created')
        if instance.previous_account_number != instance.account_number or created:
            send_mail('User changed account number', \
                'Quantrade user had change account number from {0} to {1}.\n\n'.\
                format(instance.previous_account_number, instance.account_number), \
                settings.DEFAULT_FROM_EMAIL, settings.NOTIFICATIONS_EMAILS, \
                fail_silently=False)
    except Exception as e:
        print("At sending email when account change {}".format(e))


"""
@receiver(post_save, sender=Post)
def send_email_on_acc_number_update(sender, instance, **kwargs):
    try:
        created = kwargs.get('created')
            recipients = Subscribers.objects.filter(subscribed=True)
            for reci in recipients:
                send_mail('Update from Quantrade.co.uk trading signals: {}'.format(instance.title), \
                    '{0}.\n\n--\nQuantrade Ltd.\n\nIf you don't like to receive \
                    these updates, please go to your members area and unsubscribe,
                    if you're not registered member use link below.'.format(instance.content), \
                    settings.DEFAULT_FROM_EMAIL, reci.email, \
                    fail_silently=False)
    except Exception as e:
        print("At sending email when blog post made {}".format(e))
"""

"""
@receiver(post_save, sender=Portfolios)
def recalc_strategy(sender, instance, **kwargs):
    try:
        if kwargs["created"]:
            sleep(1)
            send_mail('New user registered at Quantrade', \
                'Quantrade has new user {}.\n\n'.format(instance), \
                settings.DEFAULT_FROM_EMAIL,
                settings.NOTIFICATIONS_EMAILS, \
                fail_silently=False)

            email = instance.email
            if email:
                send_mail('You had registered at Quantrade!', \
                    'You are now the member of Quantrade quantitative trading signals where you can enjoy hundreds of profitable trading strategies for free!.\n\n', \
                    settings.DEFAULT_FROM_EMAIL,
                    [email], \
                    fail_silently=False)

    except:
        pass
"""
