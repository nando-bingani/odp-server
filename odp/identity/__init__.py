from pathlib import Path

import redis
from authlib.integrations.flask_client import OAuth
from flask import Flask
from flask_mail import Mail

from odp.config import config
from odp.lib.hydra import HydraAdminAPI
from odp.ui import base

if config.ODP.ENV != 'testing':
    # TODO: test odp.identity...

    mail = Mail()

    hydra_admin_api = HydraAdminAPI(config.HYDRA.ADMIN.URL)

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
        SECRET_KEY=config.ODP.IDENTITY.FLASK_SECRET,
        MAIL_SERVER=config.ODP.MAIL.HOST,
        MAIL_PORT=config.ODP.MAIL.PORT,
        MAIL_USE_TLS=config.ODP.MAIL.TLS,
        MAIL_USERNAME=config.ODP.MAIL.USERNAME,
        MAIL_PASSWORD=config.ODP.MAIL.PASSWORD,
    )

    base.init_app(app, template_dir=Path(__file__).parent / 'templates')
    db.init_app(app)
    views.init_app(app)
    mail.init_app(app)
    google_oauth2.init_app(app)

    return app
