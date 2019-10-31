from __future__ import absolute_import

import io
import logging

from flask import g, request, json, send_file, abort
from flask.views import MethodView
from huskar_sdk_v2.consts import (
    SERVICE_SUBDOMAIN, SWITCH_SUBDOMAIN, CONFIG_SUBDOMAIN, OVERALL)

from huskar_api import settings
from huskar_api.models import huskar_client
from huskar_api.models.comment import get_comment, set_comment
from huskar_api.models.const import ROUTE_DEFAULT_INTENT, INFRA_CONFIG_KEYS
from huskar_api.models.route.utils import parse_route_key
from huskar_api.models.instance import InstanceManagement
from huskar_api.models.utils import merge_instance_list, retry
from huskar_api.models.exceptions import MalformedDataError, OutOfSyncError
from huskar_api.models.auth import Authority
from huskar_api.service import comment as comment_facade
from huskar_api.service import config as config_facade
from huskar_api.service.admin.application_auth import (
    check_application_auth, check_application)
from huskar_api.service.utils import check_cluster_name
from huskar_api.switch import switch, SWITCH_ENABLE_CONFIG_PREFIX_BLACKLIST
from .schema import instance_schema, validate_fields
from .utils import login_required, api_response, audit_log


logger = logging.getLogger(__name__)


class InstanceFacade(object):
    client = huskar_client

    def __init__(self, subdomain, application_name, include_comment=True):
        self.subdomain = subdomain
        self.application_name = application_name
        self.include_comment = include_comment
        self.im = InstanceManagement(self.client, application_name, subdomain)
        self.im.set_context(g.application_name, g.cluster_name)

    def fetch_instance_list(self, pairs, resolve=True):
        include_comment = self.include_comment and not g.auth.is_minimal_mode
        for cluster_name, key in pairs:
            info, physical_name = self.im.get_instance(
                cluster_name, key, resolve=resolve)
            if info.stat is None:
                continue
            data = {
                'application': self.application_name,
                'cluster': cluster_name,
                'key': key,
                'value': info.data,
                'meta': self.make_meta_info(info),
            }
            if self.subdomain == SERVICE_SUBDOMAIN:
                data['runtime'] = self.make_runtime_field(info)
                if physical_name:
                    data['cluster_physical_name'] = physical_name
            if include_comment:
                comment = get_comment(
                    self.application_name, cluster_name,
                    self.subdomain, key)
                data['comment'] = comment
            yield data

    @retry(OutOfSyncError, interval=0.5, max_retry=3)
    def set_instance(self, cluster_name, key, value, comment=None,
                     overwrite=False):
        instance, _ = self.im.get_instance(
            cluster_name, key, resolve=False)
        if overwrite or instance.stat is None:
            instance.data = value
            instance.save()
            if self.include_comment and comment is not None:
                set_comment(
                    self.application_name, cluster_name,
                    self.subdomain, key, comment)
            return instance

    def get_instance(self, cluster_name, key, resolve=True):
        iterator = self.fetch_instance_list([(cluster_name, key)],
                                            resolve=resolve)
        return next(iterator, None)

    def get_instance_list(self, resolve=True):
        pairs = (
            (cluster_name, key)
            for cluster_name in self.im.list_cluster_names()
            for key in self.im.list_instance_keys(
                cluster_name, resolve=resolve)
        )
        return list(self.fetch_instance_list(pairs, resolve=resolve))

    def get_instance_list_by_cluster(self, cluster_name, resolve=True):
        keys = self.im.list_instance_keys(cluster_name, resolve=resolve)
        pairs = ((cluster_name, k) for k in keys)
        return list(self.fetch_instance_list(pairs, resolve=resolve))

    def get_merged_instance_list(self, cluster_name):
        overall_instance_list = self.get_instance_list_by_cluster(OVERALL)
        current_instance_list = self.get_instance_list_by_cluster(cluster_name)
        return merge_instance_list(
            self.application_name,
            overall_instance_list,
            current_instance_list,
            cluster_name)

    def get_cluster_list(self):
        cluster_names = self.im.list_cluster_names()
        for cluster_name in cluster_names:
            cluster_info = None
            if self.subdomain == SERVICE_SUBDOMAIN:
                try:
                    cluster_info = self.im.get_cluster_info(cluster_name)
                    meta = self.make_meta_info(cluster_info, is_cluster=True)
                except MalformedDataError as e:
                    logger.warning('Failed to parse info "%s"', e.info.path)
                    meta = self.make_meta_info(e.info, is_cluster=True)
                if cluster_info and cluster_info.data:
                    route_map = cluster_info.get_route()
                    yield {'name': cluster_name,
                           'physical_name': cluster_info.get_link(),
                           'route': sorted(self.make_route_list(route_map)),
                           'meta': meta}
                else:
                    yield {'name': cluster_name, 'physical_name': None,
                           'route': [], 'meta': meta}
            else:
                yield {'name': cluster_name}

    @classmethod
    def check_instance_key(
            cls, subdomain, application_name, cluster_name, key):
        if (subdomain == CONFIG_SUBDOMAIN and
                key in INFRA_CONFIG_KEYS.values()):
            abort(400, 'The key {key} is reserved.'.format(key=key))

        cls.check_instance_key_in_creation(
            subdomain, application_name, cluster_name, key)

    @classmethod
    def check_instance_key_in_creation(
            cls, subdomain, application_name, cluster_name, key):
        if subdomain != CONFIG_SUBDOMAIN:
            return
        if not switch.is_switched_on(
                SWITCH_ENABLE_CONFIG_PREFIX_BLACKLIST, False):
            return
        if config_facade.exists(application_name, cluster_name, key=key):
            return

        for prefix in settings.CONFIG_PREFIX_BLACKLIST:
            if key.startswith(prefix):
                abort(400, 'The key {key} starts with {prefix} is denied.'
                      .format(key=key, prefix=prefix))

    @classmethod
    def make_route_list(cls, route_map):
        for route_key, cluster_name in route_map.iteritems():
            route_key = parse_route_key(route_key)
            yield {'application_name': route_key.application_name,
                   'intent': route_key.intent,
                   'cluster_name': cluster_name}

    @classmethod
    def make_runtime_field(cls, info):
        try:
            value = json.loads(info.data)
        except (TypeError, ValueError):
            logger.warning('Failed to parse %r', info.path)
            return
        if not isinstance(value, dict):
            logger.warning('Unexpected schema of %r', info.path)
            return
        state = value.get('state') or None
        return state and json.dumps({'state': state})

    @classmethod
    def make_meta_info(cls, info, is_cluster=False):
        meta = {
            'last_modified': int(info.stat.last_modified * 1000),
            'created': int(info.stat.created * 1000),
            'version': info.stat.version,
        }
        if is_cluster:
            if info.get_link() is None:
                meta['instance_count'] = info.stat.children_count
            else:
                meta['is_symbol_only'] = info.stat.children_count == 0
        return meta


class InstanceView(MethodView):
    UPDATE_ACTION_TYPES = {
        SWITCH_SUBDOMAIN: audit_log.types.UPDATE_SWITCH,
        CONFIG_SUBDOMAIN: audit_log.types.UPDATE_CONFIG,
    }
    DELETE_ACTION_TYPES = {
        SWITCH_SUBDOMAIN: audit_log.types.DELETE_SWITCH,
        CONFIG_SUBDOMAIN: audit_log.types.DELETE_CONFIG,
    }

    def __init__(self, subdomain, facade, is_public):
        self.subdomain = subdomain
        self.facade = facade
        self.is_public = is_public

    @login_required
    def get(self, application_name, cluster_name):
        """Gets the instance list of specified application and cluster.

        The ``read`` authority is required. See :ref:`application_auth` also.

        The response looks like::

            {
              "status": "SUCCESS",
              "message": "",
              "data": [
                {
                  "application": "base.foo",
                  "cluster": "stable",
                  "key": "DB_URL",
                  "value": "mysql://",
                  "comment": "...",
                  "meta": {
                    "last_modified": 1522033534,
                    "created": 1522033534,
                    "version": 1
                  }
                }
              ]
            }

        If the ``key`` is specified, the ``data`` field in response will be
        an object directly without :js:class:`Array` around.

        :param application_name: The name of application.
        :param cluster_name: The name of cluster.
        :query key: Optional. The specified instance will be responded.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The application, cluster or key is not found.
        :status 200: The result is in the response.
        """
        if not self.is_public:
            check_application_auth(application_name, Authority.READ)
        else:
            check_application(application_name)

        check_cluster_name(cluster_name, application_name)
        facade = InstanceFacade(self.subdomain, application_name)
        key = request.args.get('key')
        validate_fields(instance_schema, {
            'application': application_name,
            'cluster': cluster_name,
            'key': key
        }, optional_fields=['key'])

        if key:
            instance = facade.get_instance(cluster_name, key)
            if instance is None:
                abort(404, '%s %s/%s/%s does not exist' % (
                    self.subdomain, application_name, cluster_name, key,
                ))
            return api_response(instance)
        else:
            instance_list = facade.get_instance_list_by_cluster(cluster_name)
            return api_response(instance_list)

    @login_required
    def post(self, application_name, cluster_name):
        """Creates or updates an instance of switch or config.

        The ``write`` authority is required. See :ref:`application_auth` also.

        .. todo:: Add conditional request support.

        :param application_name: The name of application.
        :param cluster_name: The name of cluster. ``overall`` means default
                             value of all clusters. The cluster which does not
                             exist before will be created implicitly.
        :form key: The name of instance. (e.g. ``DB_URL``)
        :form value: The value of switch or config. An usual value of switch
                     is is the ``0`` to ``100`` range.
        :form version: The version of the key, it's optional. If the version
                       was inconsistent with the current key's version, it will
                       not be saved success.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 409: This version is inconsistent with the current key's
                     version.
        :status 404: The application is not found.
        :status 400: The cluster name or key is invalid.
        :status 200: The request is successful.
        :status 409: The version is outdated, resource was modified by
                     another request.
        """
        check_application_auth(application_name, Authority.WRITE)
        check_cluster_name(cluster_name, application_name)

        key = request.values['key'].strip()
        InstanceFacade.check_instance_key(
            self.subdomain, application_name, cluster_name, key)
        value = request.values['value'].strip()
        comment = request.form.get('comment')
        version = request.form.get('version', type=int)
        validate_fields(instance_schema, {
            'application': application_name,
            'cluster': cluster_name,
            'key': key,
            'value': value,
            'comment': comment
        }, optional_fields=['comment'])

        old_data = self.facade.get_value(application_name, cluster_name, key)
        action_type = self.UPDATE_ACTION_TYPES[self.subdomain]
        with audit_log(action_type,
                       application_name=application_name,
                       cluster_name=cluster_name,
                       key=key, old_data=old_data, new_data=value):
            try:
                self.facade.create(
                    application=application_name,
                    cluster=cluster_name,
                    key=key,
                    value=value,
                    version=version)
            except OutOfSyncError:
                abort(409, 'resource is modified by another request')
        self._save_comment(application_name, cluster_name, key)
        return api_response()

    @login_required
    def put(self, application_name, cluster_name):
        return self.post(application_name, cluster_name)

    @login_required
    def delete(self, application_name, cluster_name):
        """Deletes an instance.

        The ``write`` authority is required. See :ref:`application_auth` also.

        .. note:: When you delete the last instance of a cluster, the cluster
                  will not be deleted automaticlly.

                  Use the :ref:`cluster` API if you want to delete the empty
                  cluster manually.

        :param application_name: The name of application.
        :param cluster_name: The name of cluster.
        :form key: The key of specified instance.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 404: The application is not found.
        :status 200: The request is successful.
        """
        check_application_auth(application_name, Authority.WRITE)
        check_cluster_name(cluster_name, application_name)

        key = request.values['key'].strip()
        validate_fields(instance_schema, {
            'application': application_name,
            'cluster': cluster_name,
            'key': key,
        })

        im = InstanceManagement(
            huskar_client, application_name, self.subdomain)
        instance, _ = im.get_instance(cluster_name, key, resolve=False)
        if instance.stat is None:
            abort(404, '%s %s/%s/%s does not exist' % (
                self.subdomain, application_name, cluster_name, key,
            ))

        old_data = instance.data
        with audit_log(self.DELETE_ACTION_TYPES[self.subdomain],
                       application_name=application_name,
                       cluster_name=cluster_name,
                       key=key, old_data=old_data):
            self.facade.delete(
                application_name, cluster_name, key, strict=True)
        self._delete_comment(application_name, cluster_name, key)
        return api_response()

    def _save_comment(self, application_name, cluster_name, key):
        comment = request.form.get('comment')
        if comment is None:
            return
        if g.auth.is_minimal_mode:
            return
        if comment_facade.get(
            self.subdomain, application_name, cluster_name, key
        ) != comment:
            comment_facade.save(
                self.subdomain, application_name, cluster_name, key, comment)
            return comment

    def _delete_comment(self, application_name, cluster_name, key):
        if g.auth.is_minimal_mode:
            return
        comment_facade.delete(
            self.subdomain, application_name, cluster_name, key)


class InstanceBatchView(MethodView):
    IMPORT_ACTION_TYPES = {
        SERVICE_SUBDOMAIN: audit_log.types.IMPORT_SERVICE,
        SWITCH_SUBDOMAIN: audit_log.types.IMPORT_SWITCH,
        CONFIG_SUBDOMAIN: audit_log.types.IMPORT_CONFIG,
    }

    EXPORT_FORMAT_JSON = 'json'
    EXPORT_FORMAT_FILE = 'file'

    def __init__(self, subdomain, facade, is_public=False, has_comment=False):
        self.subdomain = subdomain
        self.facade = facade
        self.is_public = is_public
        self.has_comment = has_comment

    @login_required
    def get(self):
        """Exports multiple instances of service, switch or config.

        The ``read`` authority is required. See :ref:`application_auth` also.

        The response looks like::

            {
              "status": "SUCCESS",
              "message": "",
              "data": [
                {
                  "application": "base.foo",
                  "cluster": "stable",
                  "key": "DB_URL",
                  "value": "mysql://",
                  "comment": "..."
                },
              ]
            }

        The content of ``data`` field will be responded directly if the
        ``format`` is ``file``.

        :form application: The name of application which will be exported.
        :form cluster: The name of cluster which will be exported.
        :form format: ``json`` or ``file`` which decides the content type of
                      response.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The request is successful.
        """
        application_name = request.args['application'].strip()
        cluster_name = request.args.get('cluster')
        export_format = request.args.get(
            'format', default=self.EXPORT_FORMAT_JSON)

        if self.is_public:
            check_application(application_name)
        else:
            check_application_auth(application_name, Authority.READ)

        validate_fields(instance_schema, {
            'application': application_name,
            'cluster': cluster_name,
        }, optional_fields=['cluster'])
        facade = InstanceFacade(
            self.subdomain, application_name, self.has_comment)
        if cluster_name:
            check_cluster_name(cluster_name, application_name)
            if (self.subdomain == CONFIG_SUBDOMAIN and
                    cluster_name == ROUTE_DEFAULT_INTENT and g.cluster_name):
                content = facade.get_merged_instance_list(g.cluster_name)
            else:
                content = facade.get_instance_list_by_cluster(cluster_name)
        else:
            content = facade.get_instance_list()

        if export_format == self.EXPORT_FORMAT_FILE:
            # Don't expose meta while exporting with file.
            content = [
                {k: v for k, v in item.iteritems() if k != 'meta'}
                for item in content]

            file_alike = io.BytesIO()
            file_alike.write(json.dumps(content))
            file_alike.seek(0)
            filename = '%s_backup.json' % self.subdomain
            return send_file(file_alike,
                             as_attachment=True,
                             attachment_filename=filename,
                             mimetype='application/octet-stream',
                             add_etags=False,
                             cache_timeout=0)

        if export_format == self.EXPORT_FORMAT_JSON:
            return api_response(content)

        abort(400, 'Unrecognized "format"')

    @login_required
    def post(self):
        """Imports multiple instances of service, switch or config.

        The ``write`` authority is required. See :ref:`application_auth` also.

        The data which will be imported should be included in the request body
        as ``import_file`` field in ``multipart/form-data`` encoded.

        :form import_file: The data schema is the same as the exporting API.
        :form overwrite: ``1`` if you want to overwrite existed instances.
        :<header Content-Type: ``multipart/form-data``
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The request is successful. ``import_num`` will be
                     responded also.
        """
        overwrite = request.values.get('overwrite', type=int, default=0)
        content = request.files.get('import_file', type=json.load) or {}
        content = instance_schema.load(content, many=True).data
        instance_list = self.set_instance_list(content, bool(overwrite))
        affected = sum(1 for i in instance_list if i is not None)
        audit_log.emit(
            self.IMPORT_ACTION_TYPES[self.subdomain], datalist=content,
            overwrite=bool(overwrite), affected=affected)
        return api_response({'import_num': affected})

    def set_instance_list(self, content, overwrite):
        for item in content:
            application_name = item['application']
            cluster_name = item['cluster']
            check_application_auth(application_name, Authority.WRITE)
            check_cluster_name(cluster_name, application_name)
            InstanceFacade.check_instance_key(
                self.subdomain, application_name, cluster_name, item['key'])
            self.facade.check_cluster_name_in_creation(
                application_name, cluster_name)
        for item in content:
            facade = InstanceFacade(
                self.subdomain, item['application'], self.has_comment)
            instance = facade.set_instance(
                item['cluster'], item['key'], item['value'],
                item.get('comment'), overwrite)
            yield instance


class ClusterView(MethodView):
    CREATE_ACTION_TYPES = {
        SERVICE_SUBDOMAIN: audit_log.types.CREATE_SERVICE_CLUSTER,
        SWITCH_SUBDOMAIN: audit_log.types.CREATE_SWITCH_CLUSTER,
        CONFIG_SUBDOMAIN: audit_log.types.CREATE_CONFIG_CLUSTER,
    }
    DELETE_ACTION_TYPES = {
        SERVICE_SUBDOMAIN: audit_log.types.DELETE_SERVICE_CLUSTER,
        SWITCH_SUBDOMAIN: audit_log.types.DELETE_SWITCH_CLUSTER,
        CONFIG_SUBDOMAIN: audit_log.types.DELETE_CONFIG_CLUSTER,
    }

    def __init__(self, subdomain, facade, is_public=False):
        self.subdomain = subdomain
        self.facade = facade
        self.is_public = is_public

    @login_required
    def get(self, application_name):
        """Lists clusters of specified application.

        The service resource has a bit difference to switch and config. Its
        cluster names are public. You could read them without providing
        an authorization token.

        :param application_name: The name of application (a.k.a appid).
        :<header Authorization: Huskar Token (See :ref:`token`)
        :status 200: The cluster names should be present in the response:
                     ``{"status": "SUCCESS", "data": [{"name": "stable"}]}``
                     In addition, ``physical_name`` will be present in the list
                     item like ``name`` if the resource has support for
                     **cluster linking**, and ``route`` will be present in the
                     list if the resource has support for **route**.
        """
        validate_fields(instance_schema, {'application': application_name})
        if not self.is_public:
            check_application_auth(application_name, Authority.READ)
        else:
            check_application(application_name)
        facade = InstanceFacade(self.subdomain, application_name)
        cluster_list = list(facade.get_cluster_list())
        return api_response(cluster_list)

    @login_required
    def post(self, application_name):
        """Creates an empty cluster.

        It is not necessary to create an empty cluster before placing content
        inside it. This API is intent on creating symbol clusters for linking.

        :form cluster: The name of creating cluster.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :<header Content-Type: :mimetype:`application/x-www-form-urlencoded`
        :status 400: The cluster name has a bad format.
        :status 200: Success.
        """
        check_application_auth(application_name, Authority.WRITE)
        cluster_name = request.values['cluster'].strip()
        validate_fields(instance_schema, {
            'application': application_name,
            'cluster': cluster_name
        })
        check_cluster_name(cluster_name, application_name)

        with audit_log(self.CREATE_ACTION_TYPES[self.subdomain],
                       application_name=application_name,
                       cluster_name=cluster_name):
            self.facade.create_cluster(
                application_name, cluster_name, strict=True)
        return api_response()

    @login_required
    def delete(self, application_name):
        """Deletes an empty cluster.

        :form cluster: The name of deleting cluster.
        :<header Authorization: Huskar Token (See :ref:`token`)
        :<header Content-Type: :mimetype:`application/x-www-form-urlencoded`
        :status 400: The cluster name is invalid or the deleting cluster is not
                     empty.
        :status 200: Success.
        """
        check_application_auth(application_name, Authority.WRITE)
        cluster_name = request.values['cluster'].strip()
        validate_fields(instance_schema, {
            'application': application_name,
            'cluster': cluster_name
        })
        check_cluster_name(cluster_name, application_name)

        with audit_log(self.DELETE_ACTION_TYPES[self.subdomain],
                       application_name=application_name,
                       cluster_name=cluster_name):
            self.facade.delete_cluster(
                application_name, cluster_name, strict=True)
        return api_response()
