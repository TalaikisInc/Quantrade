from server import app

import settings


def main():
    try:
        if not settings.DEV_ENV:
            ssl = {'cert': "/etc/ssl/certs/nginx-selfsigned.crt",
                    'key': "/etc/ssl/private/nginx-selfsigned.key"}
            app.run(host=settings.HOST,
                port=settings.PORT,
                sock=None,
                debug=settings.DEBUG,
                workers=settings.API_WORKERS,
                ssl=ssl,
                log_config=None)
        else:
            app.run(host=settings.HOST,
                port=settings.PORT,
                sock=None,
                debug=settings.DEBUG)
    except KeyboardInterrupt:
        pass

main()
