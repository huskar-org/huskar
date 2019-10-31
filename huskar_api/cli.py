from __future__ import absolute_import, print_function

import sys

from flask_script import Manager, prompt_pass
from flask_script.commands import ShowUrls, Clean

from .app import create_app
from .models.auth import User


manager = Manager(create_app())
manager.add_command(ShowUrls())
manager.add_command(Clean())


@manager.command
def initadmin():
    """Creates an initial user."""
    admin_user = User.get_by_name('admin')
    if admin_user:
        print('The user "admin" exists', file=sys.stderr)
        sys.exit(1)

    password = None
    while not password:
        password = prompt_pass('Password', default='').strip()
    admin_user = User.create_normal('admin', password, is_active=True)
    admin_user.grant_admin()


def main():
    manager.run()
