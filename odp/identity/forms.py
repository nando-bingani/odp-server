from wtforms import PasswordField, StringField
from wtforms.validators import email, equal_to, input_required

from odp.ui.base.forms import BaseForm


class SignupForm(BaseForm):
    name = StringField(
        label='Full name',
        validators=[input_required()],
    )
    email = StringField(
        label='Email address',
        filters=[lambda s: s.lower() if s else s],
        validators=[input_required(), email()],
    )
    password = PasswordField(
        label='Password',
        validators=[input_required(), equal_to('confirm_password', "The passwords do not match")],
    )
    confirm_password = PasswordField(
        label='Confirm password',
        validators=[input_required()],
    )


class LoginForm(BaseForm):
    email = StringField(
        label='Email address',
        filters=[lambda s: s.lower() if s else s],
        validators=[input_required(), email()],
    )
    password = PasswordField(
        label='Password',
        validators=[input_required()],
    )


class ForgotPasswordForm(BaseForm):
    email = StringField(
        label='Email address',
        filters=[lambda s: s.lower() if s else s],
        validators=[input_required(), email()],
    )


class ResetPasswordForm(BaseForm):
    password = PasswordField(
        label='Password',
        validators=[input_required(), equal_to('confirm_password', "The passwords do not match")],
    )
    confirm_password = PasswordField(
        label='Confirm password',
        validators=[input_required()],
    )


class ProfileForm(BaseForm):
    name = StringField(label='Full name')
    picture = StringField(label='Photo URL')
