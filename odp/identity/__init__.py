from pathlib import Path

import redis
from authlib.integrations.flask_client import OAuth
from flask import Flask
from flask_mail import Mail
from jinja2 import ChoiceLoader, FileSystemLoader
from werkzeug.middleware.proxy_fix import ProxyFix

from odp.config import config
from odp.lib.hydra_admin import HydraAdminClient
from odp.ui import base

if config.ODP.ENV != 'testing':
    # TODO: test odp.identity...

    mail = Mail()

    hydra_admin = HydraAdminClient(
        server_url=config.HYDRA.ADMIN.URL,
        verify_tls=config.ODP.ENV != 'development',
        remember_login_for=config.ODP.IDENTITY.LOGIN_EXPIRY,
    )

    redis_cache = redis.Redis(
        host=config.REDIS.HOST,
        port=config.REDIS.PORT,
        db=config.REDIS.DB,
        decode_responses=True,
    )

    google_oauth2 = OAuth(cache=redis_cache)
    google_oauth2.register(
        name='google',
        client_id=config.GOOGLE.CLIENT_ID,
        client_secret=config.GOOGLE.CLIENT_SECRET,
        client_kwargs={'scope': ' '.join(config.GOOGLE.SCOPE)},
        server_metadata_url=config.GOOGLE.OPENID_URI,
    )


def create_app():
    """
    Flask application factory.
    """
    from . import db, views

    app = Flask(__name__)
    app.config.update(
        SECRET_KEY=config.ODP.IDENTITY.FLASK_KEY,
        MAIL_SERVER=config.ODP.MAIL.HOST,
        MAIL_PORT=config.ODP.MAIL.PORT,
        MAIL_USE_TLS=config.ODP.MAIL.TLS,
        MAIL_USERNAME=config.ODP.MAIL.USERNAME,
        MAIL_PASSWORD=config.ODP.MAIL.PASSWORD,
        SESSION_COOKIE_NAME='idsession',  # avoid conflict with public UI session cookie on same domain
        SESSION_COOKIE_SECURE=True,
        SESSION_COOKIE_SAMESITE='Lax',
    )

    template_dir = Path(__file__).parent / 'templates'
    app.jinja_loader = ChoiceLoader([
        FileSystemLoader(template_dir),
        FileSystemLoader(base.TEMPLATE_DIR),
    ])
    app.static_folder = base.STATIC_DIR

    base.init_app(app, is_odp_client=False)
    db.init_app(app)
    views.init_app(app)
    mail.init_app(app)
    google_oauth2.init_app(app)

    # trust the X-Forwarded-* headers set by the proxy server
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_prefix=1)

    return app
