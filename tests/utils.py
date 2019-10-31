from __future__ import absolute_import


def assert_response_ok(response):
    """Test whether the response is mean success

    :param response: Response instance from ``pytest_flask.fixtures.client``
    :raise AssertionError: If test failed, will raise ``AssertionError``
    """
    assert response.status_code == 200, response.data
    assert response.json['status'] == 'SUCCESS'
    assert response.json['message'] == ''


def assert_response_status_code(response, status_code):
    assert response.status_code == status_code


def assert_semaphore_is_zero(semaphore, x):
    assert not semaphore.locked()
    for _ in range(x):
        semaphore.acquire()
    try:
        assert semaphore.locked()
    finally:
        for _ in range(x):
            semaphore.release()
