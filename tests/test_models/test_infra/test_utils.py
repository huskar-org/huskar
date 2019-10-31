from __future__ import absolute_import

from pytest import mark

from huskar_api.models.infra import extract_application_names
from huskar_api.models.infra.utils import extract_application_name


@mark.parametrize('url,result', [
    ('', None),
    ('mysql://localhost:3306/sample', None),
    ('sam+mysql:///overall', None),
    ('sam+mysql://dal.test.auto/overall', 'dal.test.auto'),
    ('sam+mysql://user:pass@dal.test.auto/overall', 'dal.test.auto'),
    ('sam+amqp://user:@#$deX^h&@rabbitmq.100010/vhost/overall',
        'rabbitmq.100010'),
])
def test_extract_application_name(url, result):
    assert extract_application_name(url) == result


def test_extract_application_names():
    assert extract_application_names([
        '',
        'mysql://localhost:3306/sample',
        'sam+mysql:///overall',
        'sam+mysql://dal.test.auto/overall',
        'sam+mysql://user:pass@dal.test.auto/overall',
    ]) == [
        'dal.test.auto',
        'dal.test.auto',
    ]

    assert extract_application_names({
        'u1': '',
        'u2': 'mysql://localhost:3306/sample',
        'u3': 'sam+mysql:///overall',
        'u4': 'sam+mysql://dal.test.auto/overall',
        'u5': 'sam+mysql://user:pass@dal.test.auto/overall',
    }) == {
        'u4': 'dal.test.auto',
        'u5': 'dal.test.auto',
    }
