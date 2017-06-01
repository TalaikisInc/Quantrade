from server import app

from server import settings


def main():
    try:
        if not settings.DEV_ENV:
            app.run(host=settings.API_HOST,
                port=settings.PORT,
                sock=None,
                debug=settings.DEBUG,
                workers=settings.API_WORKERS,
                log_config=None)
        else:
            app.run(host=settings.API_HOST,
                port=settings.PORT,
                sock=None,
                debug=settings.DEBUG)
    except KeyboardInterrupt:
        pass

main()
