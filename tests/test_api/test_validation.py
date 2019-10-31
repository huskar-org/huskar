from __future__ import absolute_import

from pytest import raises, mark

from huskar_api.api.schema import (
    instance_schema, validate_fields, ValidationError,
    service_value_schema, event_subscribe_schema
)


# TODO: move validation tests to this file

def test_validate_does_not_exists_field():
    # the set of data keys is a subset of schema fields
    validate_fields(instance_schema, {'application': 'test'})

    # the set of data keys is not subset of schema fields
    with raises(ValidationError) as ex:
        validate_fields(instance_schema, {'test': 'test'})
    assert ex.value.message == (
        'The set of fields "set([\'test\'])" is not a subset of '
        '<InstanceSchema(many=False, strict=True)>'
    )


@mark.xparametrize
def test_service_meta(meta, expected_meta):
    data = service_value_schema.load({
        'ip': '127.0.0.1',
        'port': {'main': 5000},
        'meta': meta
    }).data
    assert data['meta'] == expected_meta


@mark.xparametrize
def test_invalid_meta(meta):
    with raises(ValidationError) as ex:
        service_value_schema.load({
            'ip': '127.0.0.1',
            'port': {'main': 500},
            'meta': meta
        })
    assert ex.value.message == {'meta': [u'Not a valid mapping type.']}


@mark.xparametrize
def test_long_polling_validation(application, clusters):
    subscription = {
        'service': {application: clusters},
        'config': {application: clusters},
        'switch': {application: clusters}
    }
    data = event_subscribe_schema.load(subscription).data
    assert data == subscription


@mark.xparametrize
def test_invalid_long_polling_input(application, clusters):
    with raises(ValidationError):
        subscription = {
            'service': {application: clusters},
            'config': {application: clusters},
            'switch': {application: clusters}
        }
        event_subscribe_schema.load(subscription)


@mark.xparametrize
def test_instance(fields, optional_fields):
    validate_fields(instance_schema, fields, optional_fields)


@mark.xparametrize
def test_invalid_instance_fields(fields, optional_fields, error):
    with raises(ValidationError) as ex:
        validate_fields(instance_schema, fields, optional_fields)
    assert ex.value.message == error
