from datetime import datetime, timedelta, timezone

from odp.const import ODPScope

all_scopes = [s for s in ODPScope]


def all_scopes_excluding(scope):
    return [s for s in ODPScope if s != scope]


def assert_empty_result(response):
    assert response.status_code == 200
    assert response.json() is None


def assert_forbidden(response):
    assert response.status_code == 403
    assert response.json() == {'detail': 'Forbidden'}


def assert_not_found(response, message='Not Found'):
    assert response.status_code == 404
    assert response.json() == {'detail': message}


def assert_method_not_allowed(response):
    assert response.status_code == 405
    assert response.json() == {'detail': 'Method Not Allowed'}


def assert_conflict(response, message):
    assert response.status_code == 409
    assert response.json() == {'detail': message}


def assert_unprocessable(response, message=None, **kwargs):
    # kwargs are key-value pairs expected within 'detail'
    assert response.status_code == 422
    error_detail = response.json()['detail']
    if message is not None:
        assert error_detail == message
    for k, v in kwargs.items():
        assert error_detail[k] == v


def assert_new_timestamp(timestamp):
    # 1 hour is a bit lenient, but handy for debugging
    assert (now := datetime.now(timezone.utc)) - timedelta(minutes=60) < timestamp < now


def assert_redirect(response, url):
    assert response.is_redirect
    assert response.next_request.url == url


def assert_tag_instance_output(response, tag_input_dict, grant_type):
    json = response.json()
    user_id = tag_input_dict.get('user_id', 'odp.test.user')
    user_email = tag_input_dict.get('user_email', 'test@saeon.ac.za')
    assert response.status_code == 200
    assert json['tag_id'] == tag_input_dict['tag_id']
    assert json['user_id'] == (user_id if grant_type == 'authorization_code' else None)
    assert json['user_name'] == ('Test User' if grant_type == 'authorization_code' else None)
    assert json['user_email'] == (user_email if grant_type == 'authorization_code' else None)
    assert json['data'] == tag_input_dict['data']
    assert_new_timestamp(datetime.fromisoformat(json['timestamp']))
    assert json['cardinality'] == tag_input_dict['cardinality']
    assert json['public'] == tag_input_dict['public']
