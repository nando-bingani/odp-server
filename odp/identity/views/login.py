from flask import Blueprint, redirect, render_template, request, url_for

from odp.config import config
from odp.identity import hydra_admin_api
from odp.identity.forms import ForgotPasswordForm, LoginForm
from odp.identity.lib import get_user_profile_by_email, validate_auto_login, validate_forgot_password, validate_user_login
from odp.identity.views import decode_token, encode_token
from odp.identity.views.account import send_password_reset_email, send_verification_email
from odp.lib import exceptions as x

bp = Blueprint('login', __name__)


@bp.route('/', methods=('GET', 'POST'))
def login():
    """User login view.

    The token ensures that we can only access this view in the context
    of the Hydra login workflow.
    """
    token = request.args.get('token')
    login_request, challenge, brand, params = decode_token(token, 'login')

    user_id = None
    error = None
    form = LoginForm(request.form)

    if request.method == 'GET':
        authenticated = login_request['skip']  # indicates whether the user is already authenticated with Hydra

        # if already authenticated, we'll wind up with either a user_id or an error
        if authenticated:
            user_id = login_request['subject']
            try:
                validate_auto_login(user_id)
            except x.ODPIdentityError as e:
                # any validation error => reject login
                user_id = None
                error = e

        # if not authenticated, we'll display the login form

    else:  # POST
        if form.validate():
            email = form.email.data
            password = form.password.data
            try:
                user_id = validate_user_login(email, password)

            except x.ODPUserNotFound:
                form.email.errors.append("The email address is not associated with any user account.")

            except x.ODPNoPassword:
                form.email.errors.append("Please click the 'Log in via Google' button.")

            except x.ODPIncorrectPassword:
                form.email.errors.append("The email address and password do not match.")

            except x.ODPEmailNotVerified:
                # the login is completed via email verification
                name = get_user_profile_by_email(email)['name']
                send_verification_email(email, name, challenge, brand)
                verify_token = encode_token('login.verify', challenge, brand, email=email, name=name)
                return redirect(url_for('.verify', token=verify_token))

            except x.ODPIdentityError as e:
                # any other validation error (e.g. account locked/disabled) => reject login
                error = e

    if user_id:
        redirect_to = hydra_admin_api.accept_login_request(challenge, user_id)
    elif error:
        redirect_to = hydra_admin_api.reject_login_request(challenge, error.error_code, error.error_description)
    else:
        return render_template('login.html', form=form, token=token, brand=brand, enable_google=config.GOOGLE.ENABLE)

    return redirect(redirect_to)


@bp.route('/verify', methods=('GET', 'POST'))
def verify():
    """View for sending a verification email.

    The token ensures that we can only get here from the user login view.
    """
    token = request.args.get('token')
    login_request, challenge, brand, params = decode_token(token, 'login.verify')

    email = params.get('email')
    name = params.get('name')

    if request.method == 'POST':
        send_verification_email(email, name, challenge, brand)

    return render_template('login_verify.html', token=token, brand=brand)


@bp.route('/forgot-password', methods=('GET', 'POST'))
def forgot_password():
    """View for sending a password reset email.

    The token ensures that we can only access this view in the context
    of the Hydra login workflow.
    """
    token = request.args.get('token')
    login_request, challenge, brand, params = decode_token(token, 'login')

    form = ForgotPasswordForm(request.form)
    sent = False

    if request.method == 'POST':
        if form.validate():
            email = form.email.data
            try:
                user_id = validate_forgot_password(email)
                name = get_user_profile_by_email(email)['name']
                send_password_reset_email(email, name, challenge, brand)
                sent = True

            except x.ODPUserNotFound:
                form.email.errors.append("The email address is not associated with any user account.")

            except x.ODPIdentityError as e:
                # any other validation error => reject login
                redirect_to = hydra_admin_api.reject_login_request(challenge, e.error_code, e.error_description)
                return redirect(redirect_to)

    return render_template('forgot_password.html', form=form, token=token, brand=brand, sent=sent)
