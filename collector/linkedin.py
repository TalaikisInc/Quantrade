from django.conf import settings

from .linkedin import linkedin


authentication = linkedin.LinkedInAuthentication(settings.SOCIAL_AUTH_LINKEDIN_OAUTH2_KEY, \
    settings.SOCIAL_AUTH_LINKEDIN_OAUTH2_SECRET, \
    settings.SOCIAL_AUTH_LOGIN_REDIRECT_URL, \
    list(linkedin.PERMISSIONS.enums.values()))

print(authentication.authorization_url)
#application = linkedin.LinkedInApplication(authentication)
token = authentication.get_access_token()
application = linkedin.LinkedInApplication(token=token)

title = 'Scala for the Impatient'
summary = 'A new book has been published'
submitted_url = 'http://horstmann.com/scala/'
submitted_image_url = 'http://horstmann.com/scala/images/cover.png'
description = 'It is a great book for the keen beginners. Check it out!'

application.submit_group_post(41001, title, summary, submitted_url, submitted_image_url, description)
