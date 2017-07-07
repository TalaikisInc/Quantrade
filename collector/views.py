from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import AdminPasswordChangeForm, PasswordChangeForm
from django.contrib.auth import update_session_auth_hash
from django.contrib import messages
from django.shortcuts import render, redirect
from django.contrib.auth import views as auth_views
from django.contrib.auth import login
from django.conf import settings
from django.core.mail import send_mail
from django.http import HttpResponseRedirect

from social_django.models import UserSocialAuth
from social_django.utils import psa
from social_core.pipeline.partial import partial

from .models import QtraUser, Brokers, Contacts
from .forms import ContactForm, ContactFormUser, BrokerForm, EmailForm


def contact(request):
    logged_user_ = request.user.id

    try:
        logged_user = QtraUser.objects.get(id=logged_user_)
    except:
        logged_user = None
    
    previous_page = request.get_full_path()

    if request.method == 'POST':
        #if user is logged in
        if not (logged_user is None):
            # create a form instance and populate it with data from the request:
            form = ContactFormUser(request.POST)

            if form.is_valid():
                subject = form.cleaned_data['subject']
                message = form.cleaned_data['message']
                name = logged_user.username
                sender = logged_user.email
                #cc_myself = form.cleaned_data['cc_myself']

                #if cc_myself:
                    #recipients.append(sender)
                msg = Contacts.objects.create(user=logged_user, name=name, email=sender, subject=subject, message=message)
                msg.save()

                recipients = ['talaikis.tadas@gmail.com']
                send_mail(subject, message, sender, recipients)

                return render(request, 'registration/thanks.html', {'save_contact': True, 'previous_page': previous_page })
            else:
                return render(request, 'registration/form_error.html', {'previous_page': previous_page })
        else:
            #if no user is logged in
            form = ContactForm(request.POST)

            if form.is_valid():
                subject = form.cleaned_data['subject']
                message = form.cleaned_data['message']
                name = form.cleaned_data['name']
                sender = form.cleaned_data['sender']
                #cc_myself = form.cleaned_data['cc_myself']

                #if cc_myself:
                    #recipients.append(sender)
                machine = QtraUser.objects.get(id=1)
                msg = Contacts.objects.create(user=machine, name=name, email=sender, subject=subject, message=message)
                msg.save()

                recipients = ['talaikis.tadas@gmail.com']
                send_mail(subject, message, sender, recipients)

                return render(request, 'registration/thanks.html', {'save_contact': True, 'previous_page': previous_page })
            else:
                return render(request, 'registration/form_error.html', {'save_contact': True, 'previous_page': previous_page })
    else:
        if not (logged_user is None):
            form = ContactFormUser()
        else:
            form = ContactForm()

    return render(request, 'registration/contact.html', {'form': form})


def thanks(request):
    return render(request, 'registration/thanks.html')


@login_required
def registered(request):
    return render(request, 'registration/registered.html', {'save_data': True })


def form_error(request):
    try:
        previous_page = request.META['HTTP_REFERER']
    except:
        previous_page = None

    return render(request, 'registration/form_error.html', {'previous_page': previous_page})


def acquire_email(request, template_name='registration/require_email.html'):
    backend = request.session['partial_pipeline']['backend']
    return render(request, template_name, {"backend": backend})


@psa('social:complete')
def register_by_access_token(request, backend):
    token = request.GET.get('access_token')
    user = request.backend.do_auth(request.GET.get('access_token'))
    if user:
        login(request, user)
        return 'OK'
    else:
        return 'ERROR'


@login_required
def delete_account(request, *kwargs):
    try:
        user_slug = kwargs['user_slug']
        confirmed = kwargs['confirmed']
    except:
        user_slug = None
        confirmed = 'N'

    try:
        if confirmed == 'Y':
            user = QtraUser.objects.get(username=user_slug).delete()
        return render(request, 'registration/delete_account.html')
    except:
        return render(request, 'registration/delete_account_error.html')


@login_required
def account(request):
    user = request.user
    try:
        twitter_login = user.social_auth.get(provider='twitter')
    except UserSocialAuth.DoesNotExist:
        twitter_login = None

    try:
        facebook_login = user.social_auth.get(provider='facebook')
    except UserSocialAuth.DoesNotExist:
        facebook_login = None

    try:
        google_login = user.social_auth.get(provider='google-oauth2')
    except UserSocialAuth.DoesNotExist:
        google_login = None

    try:
        linkedin_login = user.social_auth.get(provider='linkedin-oauth2')
    except UserSocialAuth.DoesNotExist:
        linkedin_login = None

    can_disconnect = (user.social_auth.count() > 1 or user.has_usable_password())

    return render(request, 'registration/settings.html', {
        'twitter_login': twitter_login,
        'facebook_login': facebook_login,
        'google_login': google_login,
        'linkedin_login': linkedin_login,
        'can_disconnect': can_disconnect })


@login_required
def password(request):
    previous_page = settings.BASE_URL

    if request.user.has_usable_password():
        PasswordForm = PasswordChangeForm
    else:
        PasswordForm = AdminPasswordChangeForm

    if request.method == 'POST':
        form = PasswordForm(request.user, request.POST)
        if form.is_valid():
            form.save()
            update_session_auth_hash(request, form.user)
            messages.success(request, 'Your password was successfully updated!')
            #return redirect('password')
            return render(request, 'registration/password_thanks.html', {'form': form, 'previous_page': previous_page } )
        else:
            messages.error(request, 'Please correct the error below.')
    else:
        form = PasswordForm(request.user)

    return render(request, 'registration/password.html', {'form': form } )


@login_required
def email_page(request):
    logged_user_ = request.user.id
    logged_user = QtraUser.objects.get(id=logged_user_)

    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = EmailForm(request.POST)
        if form.is_valid():
            email = form.cleaned_data['email']
            logged_user.email = email
            logged_user.save()

            return HttpResponseRedirect('/registered/')
        else:
            return HttpResponseRedirect('/form_error/')
    else:
        form = EmailForm()

    return render(request, '{}/email.html'.format(settings.TEMPLATE_NAME), {'form': form, 'logged_user': logged_user })


def send_broker_notifications(logged_user):
    try:
        if logged_user.email:
            recipients = [logged_user.email]
            subject = "Quantrade received your broker account"
            message = "We had received your broker account number. In a day, your acount will be checked and activated by robot.\n\n"
            sender = settings.DEFAULT_FROM_EMAIL
            send_mail(subject, message, sender, recipients)

        recipients = [settings.NOTIFICATIONS_EMAILS]
        subject = "New user registered broekr account"
        message = "User: {1}.\n\n".format(logged_user.username)
        sender = settings.DEFAULT_FROM_EMAIL
        send_mail(subject, message, sender, recipients)
    except:
        pass


@login_required
def broker_page(request):
    logged_user_ = request.user.id
    logged_user = QtraUser.objects.get(id=logged_user_)

    if request.method == 'POST':
            # create a form instance and populate it with data from the request:
            form = BrokerForm(request.POST)
            if form.is_valid():
                account_id = form.cleaned_data['account_id']
                our_links = form.cleaned_data['our_links']
                broker_ = form.cleaned_data['broker']
                broker = Brokers.objects.get(title=broker_)
                logged_user.account_number = account_id
                logged_user.broker = broker
                logged_user.save()

                send_broker_notifications(logged_user=logged_user)

                return HttpResponseRedirect('/registered/')
            else:
                return HttpResponseRedirect('/form_error/')
    else:
        form = BrokerForm()

    return render(request, '{}/broker.html'.format(settings.TEMPLATE_NAME), {'form': form, 'logged_user': logged_user })


def login_error(request):
    return render(request, 'registration/login_error.html')


@login_required
def logout(request):
     auth_views.logout(request)
     return render(request, 'registration/logout.html', {'data': ''})


def login(request):
     auth_views.login(request)
     return render(request, 'registration/login.html')


def page_not_found(request):
    return render(request, template_name='{}/404.html'.format(settings.TEMPLATE_NAME), context=None, content_type=None, status=404, using=None)


def permission_denied(request):
    return render(request, template_name='{}/403.html'.format(settings.TEMPLATE_NAME), context=None, content_type=None, status=403, using=None)


def server_error(request):
    return render(request, template_name='{}/500.html'.format(settings.TEMPLATE_NAME), context=None, content_type=None, status=500, using=None)


def bad_request(request):
    return render(request, template_name='{}/400.html'.format(settings.TEMPLATE_NAME), context=None, content_type=None, status=400, using=None)
