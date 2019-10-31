from __future__ import print_function

import sys
import itertools

from huskar_api.models import DBSession
from huskar_api.models.auth import User


CODES = {
    'E-HUSKAR001': 'Unknown email domain',
    'E-HUSKAR002': 'Mismatched email and username',
}


def check_users(db):
    for user in db.query(User).all():
        if user.email is not None:
            if not user.email.endswith(u'@example.com'):
                yield 'E-HUSKAR001', u'%d\t%s\t%s' % (
                    user.id, user.email, user.username)
            elif user.email != u'%s@example.com' % user.username:
                yield 'E-HUSKAR002', u'%d\t%s\t%s' % (
                    user.id, user.email, user.username)


def main():
    db = DBSession()
    linters = [
        check_users(db),
    ]

    for code, info in itertools.chain.from_iterable(linters):
        desc = CODES[code]
        print(u'%s\t%s\t%s' % (code, desc, info), file=sys.stderr)
    else:
        sys.exit(0)

    sys.exit(1)


if __name__ == '__main__':
    main()
