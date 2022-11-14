from getpass import getpass

import argon2

from odp.const import ODPSystemRole
from odp.db import Session
from odp.db.models import User, UserRole


def create_admin_user():
    with Session.begin():
        while not (name := input('Full name: ')):
            pass
        while not (email := input('Email: ')):
            pass
        while not (password := getpass()):
            pass

        user = User(
            name=name,
            email=email,
            password=argon2.PasswordHasher().hash(password),
            active=True,
            verified=True,
        )
        user.save()

        user_role = UserRole(
            user_id=user.id,
            role_id=ODPSystemRole.ODP_ADMIN,
        )
        user_role.save()
