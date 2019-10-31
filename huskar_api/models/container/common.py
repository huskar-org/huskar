from __future__ import absolute_import
import re

container_regex = re.compile(r"^[a-z0-9]{64}$")


def is_container_id(key):
    """Checks an instance key is a container id or not.

    :param key: The instance key.
    :returns: ``True`` for possible container id.
    """
    if re.match(container_regex, key):
        return True
    return False
