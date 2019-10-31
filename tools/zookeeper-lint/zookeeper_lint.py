from __future__ import print_function

import sys
import json
import itertools

from kazoo.exceptions import NoNodeError
from huskar_sdk_v2.utils import combine
from huskar_api.models import huskar_client


class LintIssue(object):
    CODES = {
        'E-ZK001': 'Legacy runtime node.',
        'E-ZK002': 'Orphan runtime node.',
        'E-ZK003': 'Broken cluster link.',
        'E-ZK004': 'Broken service instance.',
    }

    def __init__(self, code, path, data):
        assert code in self.CODES
        self.code = code
        self.path = path
        self.data = data

    def __str__(self):
        return '{0}: {1}'.format(self.code, self.CODES[self.code])


def get_cluster_names(type_name):
    path = combine(huskar_client.base_path, type_name)
    application_names = huskar_client.client.get_children(path)
    for application_name in application_names:
        cluster_path = combine(
            huskar_client.base_path, type_name, application_name)
        cluster_names = huskar_client.client.get_children(cluster_path)
        for cluster_name in cluster_names:
            yield application_name, cluster_name


def get_fingerprints(type_name, application_name, cluster_name):
    path = combine(
        huskar_client.base_path, type_name, application_name, cluster_name)
    return huskar_client.client.get_children(path)


def lint_deprecated_runtime_value():
    cluster_names = get_cluster_names('service')
    for application_name, cluster_name in cluster_names:
        fingerprints = get_fingerprints(
            'service', application_name, cluster_name)
        for fingerprint in fingerprints:
            data_path = combine(
                huskar_client.base_path, 'service', application_name,
                cluster_name, fingerprint)
            runtime_path = combine(data_path, 'runtime')
            try:
                runtime_data, _ = huskar_client.client.get(runtime_path)
            except NoNodeError:
                continue
            else:
                try:
                    data, _ = huskar_client.client.get(data_path)
                except NoNodeError:
                    pass
                else:
                    try:
                        data = json.loads(data)
                    except (ValueError, TypeError):
                        pass
                    else:
                        if isinstance(data, dict) and 'state' in data:
                            yield LintIssue(
                                'E-ZK001', runtime_path, runtime_data)
                            continue
                yield LintIssue('E-ZK002', runtime_path, runtime_data)


def lint_cluster_link():
    cluster_names = get_cluster_names('service')
    for application_name, cluster_name in cluster_names:
        cluster_path = combine(
            huskar_client.base_path, 'service', application_name, cluster_name)
        data, stat = huskar_client.client.get(cluster_path)
        if not data:
            continue
        try:
            data = json.loads(data)
        except (TypeError, ValueError):
            yield LintIssue('E-ZK003', cluster_path, data)
        else:
            if isinstance(data, dict):
                link = data.get('link', [])
                if isinstance(link, list) and (0 <= len(link) <= 1):
                    continue
            yield LintIssue('E-ZK003', cluster_path, data)


def lint_service_instance():
    cluster_names = get_cluster_names('service')
    for application_name, cluster_name in cluster_names:
        fingerprints = get_fingerprints(
            'service', application_name, cluster_name)
        for fingerprint in fingerprints:
            path = combine(
                huskar_client.base_path, 'service', application_name,
                cluster_name, fingerprint)
            try:
                data, _ = huskar_client.client.get(path)
            except NoNodeError:
                pass
            else:
                try:
                    data = json.loads(data)
                except (ValueError, TypeError):
                    yield LintIssue('E-ZK004', path, data)
                else:
                    if (not isinstance(data, dict) or
                            not data.get('ip') or
                            not isinstance(data.get('port'), dict) or
                            not isinstance(data['port'].get('main'), int) or
                            data['port']['main'] <= 0):
                        yield LintIssue('E-ZK004', path, data)
                    elif 'meta' in data:
                        meta = data['meta']
                        if (not isinstance(meta, dict) or
                                not all(isinstance(v, unicode)
                                        for v in meta.values())):
                            yield LintIssue('E-ZK004', path, data)


def main():
    linters = [
        lint_deprecated_runtime_value(),
        lint_cluster_link(),
        lint_service_instance(),
    ]

    exit_code = 0
    for issue in itertools.chain.from_iterable(linters):
        exit_code = 1
        print(str(issue), file=sys.stderr)
        print('\tPATH\t{0!r}'.format(issue.path), file=sys.stderr)
        if len(repr(issue.data)) < 50:
            print('\tDATA\t{0!r}'.format(issue.data), file=sys.stderr)

    sys.exit(exit_code)


if __name__ == '__main__':
    main()
