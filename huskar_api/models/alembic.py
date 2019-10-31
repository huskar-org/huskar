from __future__ import absolute_import

# Include all models
import huskar_api.models.auth
import huskar_api.models.comment
import huskar_api.models.audit
import huskar_api.models.infra
import huskar_api.models.webhook


def get_metadata():
    return huskar_api.models.DeclarativeBase.metadata
