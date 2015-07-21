# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2013 OpenStack Foundation.
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

from neutronclient.v2_0 import client
from oslo.config import cfg

from neutron.common import constants as q_const
from neutron.common import topics
from neutron.openstack.common import importutils
from neutron.openstack.common import log as logging
from neutron.services.loadbalancer.drivers.common import agent_driver_base
from neutron.services.loadbalancer.drivers.haproxy import namespace_driver
from neutron.db.loadbalancer import loadbalancer_db as ldb
from neutron.plugins.common import constants
from neutron.extensions.loadbalancer import RESOURCE_ATTRIBUTE_MAP as attr_info
import copy

LOG = logging.getLogger(__name__)
FSLCONF = cfg.CONF.ml2_fslsdn

SERVICE_TYPE = 'crd'

class FslHaproxyOnHostPluginDriver(agent_driver_base.AgentDriverBase):
    device_driver = namespace_driver.DRIVER_NAME
    
    def __init__(self, plugin):
        if not self.device_driver:
            raise DriverNotSpecified()
        self.agent_rpc = agent_driver_base.LoadBalancerAgentApi(topics.LOADBALANCER_AGENT)
        self.plugin = plugin
        self._set_callbacks_on_plugin()
        self.plugin.agent_notifiers.update(
            {q_const.AGENT_TYPE_LOADBALANCER: self.agent_rpc})
        self.pool_scheduler = importutils.import_object(
            cfg.CONF.loadbalancer_pool_scheduler_driver)
        #configure CRD client
        LOG.info(_("Initializing CRD client... "))
        crd_client_params = {
            'username': FSLCONF.crd_user_name,
            'tenant_name': FSLCONF.crd_tenant_name,
            'region_name': FSLCONF.crd_region_name,
            'password': FSLCONF.crd_password,
            'auth_url': FSLCONF.crd_auth_url,
            'auth_strategy': FSLCONF.crd_auth_strategy,
            'endpoint_url': FSLCONF.crd_url,
            'timeout': FSLCONF.crd_url_timeout,
            'insecure': FSLCONF.crd_api_insecure,
            'service_type': SERVICE_TYPE,
            'ca_cert': FSLCONF.crd_ca_certificates_file,
        }
        self._crdclient = client.Client(**crd_client_params)         

    def create_vip(self, context, vip):
        vip = self.prepare_request_body('vips', 'create', vip)
        crd_vip = self._crdclient.create_vip(vip)
        if crd_vip:
            self.plugin.update_status(context, ldb.Vip, crd_vip['vip']['id'], constants.ACTIVE)


    def update_vip(self, context, old_vip, vip):
        if vip['status'] in constants.ACTIVE_PENDING_STATUSES:
            update_vip = self.prepare_request_body('vips', 'update', vip)
            crd_vip = self._crdclient.update_vip(vip['id'],update_vip)
            if crd_vip:
                self.plugin.update_status(context, ldb.Vip, vip['id'], constants.ACTIVE)
        else:
            crd_vip = self._crdclient.delete_vip(vip['id'])
            if crd_vip:
                self.plugin.update_status(context, ldb.Vip, vip['id'], constants.ACTIVE)
 
    def delete_vip(self, context, vip):
        self.plugin._delete_db_vip(context, vip['id'])
        self._crdclient.delete_vip(vip['id'])

    def create_pool(self, context, pool):
        agent = self.pool_scheduler.schedule(self.plugin, context, pool,
                                             self.device_driver)
        pool = self.prepare_request_body('pools', 'create', pool)
        crd_pool = self._crdclient.create_pool(pool)
        if crd_pool:
            self.plugin.update_status(context, ldb.Pool, crd_pool['pool']['id'], constants.ACTIVE)

    def update_pool(self, context, old_pool, pool):
        if pool['status'] in constants.ACTIVE_PENDING_STATUSES:
            update_pool = self.prepare_request_body('pools', 'update', pool)
            crd_pool = self._crdclient.update_pool(pool['id'],update_pool)
            if crd_pool:
                self.plugin.update_status(context, ldb.Pool, pool['id'], constants.ACTIVE)
        else:
            crd_pool = self._crdclient.delete_pool(pool['id'])
            if crd_pool:
                self.plugin.update_status(context, ldb.Pool, pool['id'], constants.ACTIVE)


    def delete_pool(self, context, pool):
        self.plugin._delete_db_pool(context, pool['id'])
        self._crdclient.delete_pool(pool['id'])

    def create_member(self, context, member):
        member = self.prepare_request_body('members', 'create', member)
        crd_member = self._crdclient.create_member(member)
        if crd_member:
            self.plugin.update_status(context, ldb.Member, crd_member['member']['id'], constants.ACTIVE)

    def update_member(self, context, old_member, member):
        # member may change pool id
        if member['pool_id'] != old_member['pool_id']:
            old_pool_agent = self.plugin.get_lbaas_agent_hosting_pool(
                context, old_member['pool_id'])
            if old_pool_agent:
                self._crdclient.delete_member(old_member['id'])
            member = self.prepare_request_body('members', 'create', member)
            crd_member = self._crdclient.create_member(member)
            if crd_member:
                self.plugin.update_status(context, ldb.Member, member['id'], constants.ACTIVE)
        else:
            update_member = self.prepare_request_body('members', 'update', member)
            crd_member = self._crdclient.update_member(member['id'],update_member)
            if crd_member:
                self.plugin.update_status(context, ldb.Member, member['id'], constants.ACTIVE)

    def delete_member(self, context, member):
        self.plugin._delete_db_member(context, member['id'])
        self._crdclient.delete_member(member['id'])

    def create_pool_health_monitor(self, context, healthmon, pool_id):
        new_healthmon = self.prepare_request_body('health_monitors', 'create', healthmon)
        self._crdclient.create_health_monitor(new_healthmon)
        self._crdclient.associate_health_monitor(pool_id, {'health_monitor':{'id': healthmon['id']}})

    def update_pool_health_monitor(self, context, old_health_monitor,
                                   health_monitor, pool_id):
        self._crdclient.update_health_monitor(health_monitor['id'],health_monitor)
    
    def delete_pool_health_monitor(self, context, health_monitor, pool_id):
        self.plugin._delete_db_pool_health_monitor(
            context, health_monitor['id'], pool_id
        )
        self._crdclient.disassociate_health_monitor(pool_id, health_monitor['id'])
        self._crdclient.delete_health_monitor(health_monitor['id'])


    def stats(self, context, pool_id):
        pass
    
    
    def prepare_request_body(self, resource, method, data):
        new_data = {}
        resource_info = copy.deepcopy(attr_info[resource])
        for attr, attr_val in resource_info.iteritems():
            if data.has_key(attr):
                if method == 'create':
                    if attr == 'id' or attr_val['allow_post']:
                        new_data[attr] = data[attr]
                elif method == 'update' and attr_val['allow_put']:
                    new_data[attr] = data[attr]
        
        return {resource[:-1]: new_data}