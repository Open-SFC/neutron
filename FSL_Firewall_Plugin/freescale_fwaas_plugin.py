# Copyright 2014 Freescale, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

from neutronclient.v2_0 import client
from oslo.config import cfg

from neutron.common import exceptions
#from neutron.common import rpc
from neutron.openstack.common import rpc
from neutron.common import topics
from neutron.db import api as qdbapi
from neutron.db.firewall import firewall_db
from neutron.openstack.common import excutils
from neutron.openstack.common import log as logging
from neutron.plugins.common import constants as const
from neutron.services.firewall import fwaas_plugin

LOG = logging.getLogger(__name__)

# Freescale FWaaS required configurations.
fsl_fwaas_opts = [
    cfg.StrOpt('crd_user_name', default='crd',
               help=_("CRD service Username")),
    cfg.StrOpt('crd_password', default='password',
               secret='True',
               help=_("CRD Service Password")),
    cfg.StrOpt('crd_tenant_name', default='service',
               help=_("CRD Tenant Name")),
    cfg.StrOpt('crd_auth_url',
               default='http://127.0.0.1:5000/v2.0/',
               help=_("CRD Auth URL")),
    cfg.StrOpt('crd_url',
               default='http://127.0.0.1:9797',
               help=_("URL for connecting to CRD service")),
    cfg.IntOpt('crd_url_timeout',
               default=30,
               help=_("Timeout value for connecting to "
                      "CRD service in seconds")),
    cfg.BoolOpt('crd_api_insecure',
                default=False,
                help=_("If set, ignore any SSL validation issues")),
    cfg.StrOpt('crd_auth_strategy',
               default='keystone',
               help=_("Auth strategy for connecting to "
                      "neutron in admin context")),
    cfg.StrOpt('crd_ca_certificates_file',
               help=_("Location of ca certificates file to use for "
                      "CRD client requests.")),
]
cfg.CONF.register_opts(fsl_fwaas_opts, "ml2_fslsdn")
# shortcut
FSLCONF = cfg.CONF.ml2_fslsdn
SERVICE_TYPE = 'crd'


def CrdClient():
    """Initialize CRD client."""
    crd_client_params = {
        'username': FSLCONF.crd_user_name,
        'tenant_name': FSLCONF.crd_tenant_name,
        'password': FSLCONF.crd_password,
        'auth_url': FSLCONF.crd_auth_url,
        'auth_strategy': FSLCONF.crd_auth_strategy,
        'endpoint_url': FSLCONF.crd_url,
        'timeout': FSLCONF.crd_url_timeout,
        'insecure': FSLCONF.crd_api_insecure,
        'service_type': SERVICE_TYPE,
        'ca_cert': FSLCONF.crd_ca_certificates_file,
    }
    return client.Client(**crd_client_params)


class FirewallCallbacks(fwaas_plugin.FirewallCallbacks):

    """Callbacks to handle CRD notifications to amqp."""

    RPC_API_VERSION = '1.0'

    def __init__(self, plugin):
        self._client = CrdClient()
        self.plugin = plugin
        self.fw = fwaas_plugin.FirewallPlugin()

    def get_firewalls_for_tenant(self, context, **kwargs):
        """Get all Firewalls and rules for a tenant from CRD."""

        fw_list = []
        for fw in self.plugin.get_firewalls(context):
            fw_id = fw['id']
            # get the firewall details from CRD service.
            crd_fw_details = self._client.show_firewall(fw_id)
            config_mode = crd_fw_details['firewall']['config_mode']
            # get those FWs with config mode NetworkNode (NN) or None
            if config_mode == 'NN' or config_mode is None:
                fw_list.append(self.plugin._make_firewall_dict_with_rules(
                    context, fw_id))
        return fw_list


class FirewallPlugin(firewall_db.Firewall_db_mixin):

    """Implementation of the Freescale Firewall Service Plugin.

    This class manages the workflow of FWaaS request/response.
    Existing Firewall database is used.
    """
    supported_extension_aliases = ["fwaas"]

    def __init__(self):
        """Do the initialization for the firewall service plugin here."""

        qdbapi.register_models()
        self._client = CrdClient()
        #self.endpoints = [FirewallCallbacks(self)]

        #self.conn = rpc.create_connection(new=True)
        #self.conn.create_consumer(
        #    topics.FIREWALL_PLUGIN, self.endpoints, fanout=False)
        #self.conn.consume_in_thread()
        self.callbacks = FirewallCallbacks(self)

        self.conn = rpc.create_connection(new=True)
        self.conn.create_consumer(
            topics.FIREWALL_PLUGIN,
            self.callbacks.create_rpc_dispatcher(),
            fanout=False)
        self.conn.consume_in_thread()

    def _update_firewall(self, context, firewall_id):
        status_update = {"firewall": {"status": const.PENDING_UPDATE}}
        super(FirewallPlugin, self).update_firewall(context, firewall_id,
                                                    status_update)
        try:
            self._client.update_firewall(firewall_id, status_update)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed to update firewall (%s)."), firewall_id)

    def _update_firewall_policy(self, context, firewall_policy_id):
        firewall_policy = self.get_firewall_policy(context, firewall_policy_id)
        if firewall_policy:
            for firewall_id in firewall_policy['firewall_list']:
                self._update_firewall(context, firewall_id)

    # Firewall Management
    def create_firewall(self, context, firewall):
        firewall['firewall']['status'] = const.PENDING_CREATE
        fw = super(FirewallPlugin, self).create_firewall(context, firewall)
        try:
            crd_firewall = {'firewall': fw}
            self._client.create_firewall(crd_firewall)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed to create firewall."))
                fw_id = fw['firewall']['id']
                super(FirewallPlugin, self).delete_firewall(context, fw_id)
        return fw

    def update_firewall(self, context, id, firewall):
        firewall['firewall']['status'] = const.PENDING_UPDATE
        fw = super(FirewallPlugin, self).update_firewall(context,
                                                         id,
                                                         firewall)
        try:
            crd_firewall = {'firewall': fw}
            self._client.update_firewall(id, crd_firewall)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed to update firewall (%s)."), id)
        return fw

    def delete_db_firewall_object(self, context, id):
        firewall = self.get_firewall(context, id)
        if firewall['status'] in [const.PENDING_DELETE]:
            super(FirewallPlugin, self).delete_firewall(context, id)

    def delete_firewall(self, context, id):
        status_update = {"firewall": {"status": const.PENDING_DELETE}}
        super(FirewallPlugin, self).update_firewall(context, id,
                                                    status_update)
        try:
            self._client.delete_firewall(id)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed to delete firewall (%s)."), id)

    # Firewall Policy Management
    def create_firewall_policy(self, context, firewall_policy):
        fwp = super(
            FirewallPlugin,
            self).create_firewall_policy(
            context,
            firewall_policy)
        fwp.pop('firewall_list')
        try:
            crd_firewall_policy = {'firewall_policy': fwp}
            self._client.create_firewall_policy(crd_firewall_policy)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed to create firewall policy."))
                fwp_id = fwp['firewall_policy']['id']
                super(FirewallPlugin, self).delete_firewall_policy(context,
                                                                   fwp_id)
        return fwp

    def update_firewall_policy(self, context, id, firewall_policy):
        fwp = super(FirewallPlugin,
                    self).update_firewall_policy(context, id, firewall_policy)
        self._update_firewall_policy(context, id)
        fwp.pop('firewall_list')
        try:
            crd_firewall_policy = {'firewall_policy': fwp}
            self._client.update_firewall_policy(id, crd_firewall_policy)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Update firewall policy failed (%s)."), id)
        return fwp

    def delete_firewall_policy(self, context, id):
        super(FirewallPlugin, self).delete_firewall_policy(context, id)
        try:
            self._client.delete_firewall_policy(id)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("CRD Error: delete_firewall_policy failed."))

    # Firewall Rule management
    def create_firewall_rule(self, context, firewall_rule):
        fwr = super(FirewallPlugin,
                    self).create_firewall_rule(context, firewall_rule)
        try:
            crd_firewall_rule = {'firewall_rule': fwr}
            self._client.create_firewall_rule(crd_firewall_rule)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed to create firewall rule."))
                fwr_id = fwr['firewall_rule']['id']
                super(FirewallPlugin, self).delete_firewall_rule(context,
                                                                 fwr_id)
        return fwr

    def update_firewall_rule(self, context, id, firewall_rule):
        fwr = super(FirewallPlugin,
                    self).update_firewall_rule(context, id, firewall_rule)
        if fwr['firewall_policy_id']:
            self._update_firewall_policy(context, fwr['firewall_policy_id'])
        try:
            crd_firewall_rule = {'firewall_rule': fwr}
            self._client.update_firewall_rule(id, crd_firewall_rule)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed to update firewall rule (%s)."), id)
        return fwr

    def delete_firewall_rule(self, context, id):
        fwr = self.get_firewall_rule(context, id)
        super(FirewallPlugin, self).delete_firewall_rule(context, id)
        if fwr['firewall_policy_id']:
            self._update_firewall_policy(context, fwr['firewall_policy_id'])
        try:
            self._client.delete_firewall_rule(id)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed to delete firewall rule."))

    def insert_rule(self, context, id, rule_info):
        fwp = super(FirewallPlugin,
                    self).insert_rule(context, id, rule_info)
        try:
            self._client.firewall_policy_insert_rule(id, rule_info)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed to insert rule %(rule)s into"
                            "firewall policy %(fwpid)s."),
                          {'rule': rule_info,
                           'fwpid': id})
                super(FirewallPlugin, self).remove_rule(context, id,
                                                        rule_info)
        self._update_firewall_policy(context, id)
        return fwp

    def remove_rule(self, context, id, rule_info):
        fwp = super(FirewallPlugin,
                    self).remove_rule(context, id, rule_info)
        try:
            self._client.firewall_policy_remove_rule(id, rule_info)
        except CrdServerException:
            with excutils.save_and_reraise_exception():
                LOG.error(_("Failed to remove rule %(rule)s from"
                            "firewall policy %(fwpid)s."),
                          {'rule': rule_info,
                           'fwpid': id})
                super(FirewallPlugin, self).insert_rule(context, id,
                                                        rule_info)
        self._update_firewall_policy(context, id)
        return fwp


class CrdServerException(exceptions.NeutronException):
    message = _("CRD Error: An unknown exception %(status)s occurred:"
                "%(response)s.")

    def __init__(self, **kwargs):
        super(CrdServerException, self).__init__(**kwargs)

        self.status = kwargs.get('status')
        self.header = kwargs.get('header')
        self.response = kwargs.get('response')
