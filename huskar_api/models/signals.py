from __future__ import absolute_import

from blinker import Namespace


namespace = Namespace()

team_will_be_archived = namespace.signal('team_will_be_archived')
team_will_be_deleted = namespace.signal('team_will_be_deleted')
session_load_user_failed = namespace.signal('session_load_user_failed')
new_action_detected = namespace.signal('conecerd_action_detected')
user_grant_admin = namespace.signal('user_grant_admin')
user_dismiss_admin = namespace.signal('user_dismiss_admin')
