from django.shortcuts import redirect
from django.core import signing
from django.core.mail import EmailMultiAlternatives
from django.core.files.base import ContentFile
from django.conf import settings

from os.path import join
import urllib.request, urllib.error, urllib.parse
from social.pipeline.partial import partial
#from social.pipeline.user import USER_FIELDS
from social.backends.google import GoogleOAuth2
from social.backends.facebook import FacebookOAuth2
from social.backends.twitter import TwitterOAuth
from social.exceptions import (AuthException, AuthFailed, AuthCanceled, \
        AuthUnknownError, AuthTokenError, AuthMissingParameter, AuthStateMissing, \
        AuthStateForbidden, AuthTokenRevoked, AuthForbidden, \
        AuthUnreachableProvider, InvalidEmail, AuthAlreadyAssociated)

from .models import QtraUser
from .views import acquire_email, login_error


def get_avatar(backend, user, response, *args, **kwargs):
    try:
        if isinstance(backend, GoogleOAuth2):
            if response.get('image') and response['image'].get('url'):
                url = response['image'].get('url')
                ext = url.split('.')[2].replace('/', '')
                user.avatar.save(join('google', '{0}.{1}'.format(ext, 'jpg')), ContentFile(urllib.request.urlopen(url).read()), save=False)

            #email
            if user.email:
                pass
            else:
                try:
                    email = response['emails'][0]['value']
                    user.email = email
                except:
                    pass

            #description
            if user.userprofile:
                pass
            else:
                try:
                    user.userprofile = response['aboutMe']
                except:
                    pass

            #dispaly name
            if user.g_display_name:
                pass
            else:
                try:
                    display_name = response['displayName']
                    user.g_display_name = display_name
                except:
                    pass

            #gender
            if user.gender:
                pass
            else:
                try:
                    user.gender = response['gender']
                except:
                    pass

            #skills
            if user.skills:
                pass
            else:
                try:
                    user.skills = response['skills']
                except:
                    pass

            #skills
            if user.occupation:
                pass
            else:
                try:
                    user.occupation = response['occupation']
                except:
                    pass

            #g+
            if user.gplus_url:
                pass
            else:
                try:
                    user.gplus_url = response['url']
                except:
                    pass

            user.save()

        if isinstance(backend, FacebookOAuth2):

            if user.avatar:
                pass
            else:
                try:
                    url = 'http://graph.facebook.com/{0}/picture'.format(response['id'])
                    ext = url.split('.')[-1].split('/')[-2]
                    user.avatar.save(join('facebook', '{0}.jpg'.format(ext)), ContentFile(urllib.request.urlopen(url).read()), save=False)
                except:
                    pass

            #email
            if user.email:
                pass
            else:
                try:
                    user.email = response['email']
                except:
                    pass

            #age range
            if user.age_range:
                pass
            else:
                try:
                    user.age_range = response['age_range']
                except:
                    pass

            #name
            #if user.nickname:
                #pass
            #else:
                #try:
                    #nickname = response['name']
                    #user.nickname = nickname
                #except:
                    #pass

            user.save()

        if isinstance(backend, TwitterOAuth):

            #email
            if user.email:
                pass
            else:
                try:
                    email = response['email']
                    user.email = email
                except:
                    pass

            #name
            #if user.nickname:
                #pass
            #else:
                #try:
                    #nickname = response['name']
                    #user.nickname = nickname
                #except:
                    #pass

            #location
            if user.location:
                pass
            else:
                try:
                    time_zone = response['time_zone']
                    user.location = time_zone
                except:
                    pass

            try:
                user.twitter_followers = response['followers_count']
            except:
                pass

            user.save()
    except (AuthException, AuthFailed, AuthCanceled, \
            AuthUnknownError, AuthTokenError, AuthMissingParameter, AuthStateMissing, \
            AuthStateForbidden, AuthTokenRevoked, AuthForbidden, \
            AuthUnreachableProvider, InvalidEmail, AuthAlreadyAssociated) as e:
        return redirect(login_error)


def redirect_if_no_refresh_token(backend, response, social, *args, **kwargs):
    if backend.name == 'google-oauth2' and social and \
       response.get('refresh_token') is None and \
       social.extra_data.get('refresh_token') is None:
        return redirect('/login/google-oauth2?approval_prompt=force')


def SendVerificationEmail(strategy, backend, code):
    signature = signing.dumps({"session_key": strategy.session.session_key, "email": code.email},
                              key=settings.EMAIL_SECRET_KEY)
    verifyURL = "{0}?verification_code={1}&signature={2}".format(
        reverse('social:complete', args=(backend.name,)),
        code.code, signature)
    verifyURL = strategy.request.build_absolute_uri(verifyURL)

    emailHTML = "email html collector.social_profile"# Include your function that returns an html string here
    emailText = """Welcome to MyApp!
            In order to login with your new user account, you need to verify your email address with us.
            Please copy and paste the following into your browser's url bar: {verifyURL}
            """.format(verifyURL=verifyURL)

    kwargs = {
        "subject": "Verify Your Account",
        "body": emailText,
        "from_email": "MyApp <noreply@myapp.com>",
        "to": ["recipient@email.address"],
    }

    email = EmailMultiAlternatives(**kwargs)
    email.attach_alternative(emailHTML, "text/html")
    email.send()


def require_email(strategy, details, user=None, is_new=False, *args, **kwargs):
    backend = kwargs.get('backend')

    if user and user.email:
        return # The user we're logging in already has their email attribute set
    elif is_new and not details.get('email'):
        # If we're creating a new user, and we can't find the email in the details
        # we'll attempt to request it from the data returned from our backend strategy
        userEmail = strategy.request_data().get('email')
        if userEmail:
            details['email'] = userEmail
        else:
            # If there's no email information to be had, we need to ask the user to fill it in
            # This should redirect us to a view
            return redirect(acquire_email)
