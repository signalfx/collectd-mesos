#! /usr/bin/python
# Copyright 2014 Ray Rodriguez
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collectd
import json
import urllib2
import socket
import collections

PREFIX = "mesos"
MESOS_HOST = "localhost"
MESOS_PORT = 5050
MESOS_URL = ""
VERBOSE_LOGGING = False

Stat = collections.namedtuple('Stat', ('type', 'path'))

STATS_CUR = {}

# DICT: Mesos 0.19.0
STATS_MESOS = {
    # General
    'activated_slaves': Stat("gauge", "activated_slaves"),
    'active_schedulers': Stat("gauge", "active_schedulers"),
    'active_tasks_gauge': Stat("gauge", "active_tasks_gauge"),
    'cpus_percent': Stat("gauge", "cpus_percent"),
    'cpus_total': Stat("gauge", "cpus_total"),
    'cpus_used': Stat("gauge", "cpus_used"),
    'deactivated_slaves': Stat("gauge", "deactivated_slaves"),
    'disk_percent': Stat("gauge", "disk_percent"),
    'disk_total_in_mb': Stat("gauge", "disk_total"),
    'disk_used_in_mb': Stat("gauge", "disk_used"),
    'elected': Stat("gauge", "elected"),
    'failed_tasks': Stat("gauge", "failed_tasks"),
    'finished_tasks': Stat("gauge", "finished_tasks"),
    'invalid_status_updates': Stat("gauge", "invalid_status_updates"),
    'killed_tasks': Stat("gauge", "killed_tasks"),
    'mem_percent': Stat("gauge", "mem_percent"),
    'mem_total_in_mb': Stat("gauge", "mem_total"),
    'mem_used_in_mb': Stat("gauge", "mem_used"),
    'outstanding_offers': Stat("gauge", "outstanding_offers"),
    'staged_tasks': Stat("gauge", "staged_tasks"),
    'started_tasks': Stat("gauge", "started_tasks"),
    'total_schedulers': Stat("gauge", "total_schedulers"),
    'uptime': Stat("counter", "uptime"),
    'valid_status_updates': Stat("gauge", "valid_status_updates"),

    # Master
    'master/cpus_percent': Stat("gauge", "master/cpus_percent"),
    'master/cpus_total': Stat("gauge", "master/cpus_total"),
    'master/cpus_used': Stat("gauge", "master/cpus_used"),
    'master/disk_percent': Stat("gauge", "master/disk_percent"),
    'master/disk_total_in_mb': Stat("gauge", "master/disk_total"),
    'master/disk_used_in_mb': Stat("gauge", "master/disk_used"),
    'master/dropped_messages': Stat("counter", "master/dropped_messages"),
    'master/event_queue_size': Stat("gauge", "master/event_queue_size"),
    'master/frameworks_active': Stat("gauge", "master/frameworks_active"),
    'master/frameworks_inactive': Stat("gauge", "master/frameworks_inactive"),
    'master/invalid_framework_to_executor_messages': Stat("gauge", "master/invalid_framework_to_executor_messages"),
    'master/invalid_status_update_acknowledgements': Stat("gauge", "master/invalid_status_update_acknowledgements"),
    'master/invalid_status_updates': Stat("gauge", "master/invalid_status_updates"),
    'master/mem_percent': Stat("gauge", "master/mem_percent"),
    'master/mem_total_in_mb': Stat("gauge", "master/mem_total"),
    'master/mem_used_in_mb': Stat("gauge", "master/mem_used"),
    'master/messages_authenticate': Stat("gauge", "master/messages_authenticate"),
    'master/messages_deactivate_framework': Stat("gauge", "master/messages_deactivate_framework"),
    'master/messages_exited_executor': Stat("gauge", "master/messages_exited_executor"),
    'master/messages_framework_to_executor': Stat("gauge", "master/messages_framework_to_executor"),
    'master/messages_kill_task': Stat("gauge", "master/messages_kill_task"),
    'master/messages_launch_tasks': Stat("gauge", "master/messages_launch_tasks"),
    'master/messages_reconcile_tasks': Stat("gauge", "master/messages_reconcile_tasks"),
    'master/messages_register_framework': Stat("gauge", "master/messages_register_framework"),
    'master/messages_register_slave': Stat("gauge", "master/messages_register_slave"),
    'master/messages_reregister_framework': Stat("gauge", "master/messages_reregister_framework"),
    'master/messages_reregister_slave': Stat("gauge", "master/messages_reregister_slave"),
    'master/messages_revive_offers': Stat("gauge", "master/messages_revive_offers"),
    'master/messages_status_update': Stat("gauge", "master/messages_status_update"),
    'master/messages_status_update_acknowledgement': Stat("gauge", "master/messages_status_update_acknowledgement"),
    'master/messages_unregister_framework': Stat("gauge", "master/messages_unregister_framework"),
    'master/messages_unregister_slave': Stat("gauge", "master/messages_unregister_slave"),
    'master/outstanding_offers': Stat("gauge", "master/outstanding_offers"),
    'master/recovery_slave_removals': Stat("gauge", "master/recovery_slave_removals"),
    'master/slave_registrations': Stat("gauge", "master/slave_registrations"),
    'master/slave_removals': Stat("gauge", "master/slave_removals"),
    'master/slave_reregistrations': Stat("gauge", "master/slave_reregistrations"),
    'master/slaves_active': Stat("gauge", "master/slaves_active"),
    'master/tasks_failed': Stat("gauge", "master/tasks_failed"),
    'master/tasks_finished': Stat("gauge", "master/tasks_finished"),
    'master/tasks_killed': Stat("gauge", "master/tasks_killed"),
    'master/tasks_lost': Stat("gauge", "master/tasks_lost"),
    'master/tasks_running': Stat("gauge", "master/tasks_running"),
    'master/tasks_staging': Stat("gauge", "master/tasks_staging"),
    'master/tasks_starting': Stat("gauge", "master/tasks_starting"),
    'master/uptime_secs': Stat("counter", "master/uptime_secs"),
    'master/valid_framework_to_executor_messages': Stat("gauge", "master/valid_framework_to_executor_messages"),
    'master/valid_status_update_acknowledgements': Stat("gauge", "master/valid_status_update_acknowledgements"),
    'master/valid_status_updates': Stat("gauge", "master/valid_status_updates"),

    # Registrar
    'registrar/queued_operations': Stat("gauge", "registrar/queued_operations"),
    'registrar/registry_size_bytes': Stat("bytes", "registrar/registry_size_bytes"),
    'registrar/state_fetch_in_ms': Stat("gauge", "registrar/state_fetch_ms"),
    'registrar/state_store_in_ms': Stat("gauge", "registrar/state_store_ms"),
    'registrar/state_store_in_ms/count': Stat("gauge", "registrar/state_store_ms/count"),
    'registrar/state_store_in_ms/max': Stat("gauge", "registrar/state_store_ms/max"),
    'registrar/state_store_in_ms/min': Stat("gauge", "registrar/state_store_ms/min"),
    'registrar/state_store_in_ms/p50': Stat("gauge", "registrar/state_store_ms/p50"),
    'registrar/state_store_in_ms/p94': Stat("gauge", "registrar/state_store_ms/p95"),
    'registrar/state_store_in_ms/p99': Stat("gauge", "registrar/state_store_ms/p99"),
    'registrar/state_store_in_ms/p999': Stat("gauge", "registrar/state_store_ms/p999"),
    'registrar/state_store_in_ms/p9999': Stat("gauge", "registrar/state_store_ms/p9999"),

    # Master System Metrics
    'system/cpus_total': Stat("gauge", "system/cpus_total"),
    'system/load_15min': Stat("gauge", "system/load_15min"),
    'system/load_1min': Stat("gauge", "system/load_1min"),
    'system/load_5min': Stat("gauge", "system/load_5min"),
    'system/mem_free_bytes': Stat("bytes", "system/mem_free_bytes"),
    'system/mem_total_bytes': Stat("bytes", "system/mem_total_bytes"),
}

# FUNCTION: Collect stats from JSON result
def lookup_stat(stat, json):
    val = dig_it_up(json, STATS_CUR[stat].path)

    # Check to make sure we have a valid result
    # dig_it_up returns False if no match found
    if not isinstance(val, bool):
        return int(val)
    else:
        return None


def configure_callback(conf):
    """Received configuration information"""
    global MESOS_HOST, MESOS_PORT, MESOS_URL, VERBOSE_LOGGING, STATS_CUR
    if len(conf) > 0:
        for node in conf.children:
            if node.key == 'Host':
                MESOS_HOST = node.values[0]
            elif node.key == 'Port':
                MESOS_PORT = int(node.values[0])
            elif node.key == 'Verbose':
                VERBOSE_LOGGING = bool(node.values[0])
            elif node.key == "Version":
                MESIS_VERSION = node.values[0]
            else:
                collectd.warning('mesos plugin: Unknown config key: %s.' % node.key)

    MESOS_URL = "http://" + MESOS_HOST + ":" + str(MESOS_PORT) + "/stats.json"
    STATS_CUR = dict(STATS_MESOS.items())

    log_verbose('Configured with host=%s, port=%s, url=%s' % (MESOS_HOST, MESOS_PORT, MESOS_URL))


def fetch_stats():
    try:
        result = json.load(urllib2.urlopen(MESOS_URL, timeout=10))
    except urllib2.URLError, e:
        collectd.error('mesos plugin: Error connecting to %s - %r' % (MESOS_URL, e))
        return None
    return parse_stats(result)


def parse_stats(json):
    """Parse stats response from Mesos"""
    for name, key in STATS_CUR.iteritems():
        result = lookup_stat(name, json)
        dispatch_stat(result, name, key)


def dispatch_stat(result, name, key):
    """Read a key from info response data and dispatch a value"""
    if result is None:
        collectd.warning('mesos plugin: Value not found for %s' % name)
        return
    estype = key.type
    value = int(result)
    log_verbose('Sending value[%s]: %s=%s' % (estype, name, value))

    val = collectd.Values(plugin='mesos')
    val.type = estype
    val.type_instance = name
    val.values = [value]
    val.dispatch()


def read_callback():
    log_verbose('Read callback called')
    stats = fetch_stats()


def dig_it_up(obj, path):
    try:
        if type(path) in (str, unicode):
            path = path.split('.')
        return reduce(lambda x, y: x[y], path, obj)
    except:
        return False


def log_verbose(msg):
    if not VERBOSE_LOGGING:
        return
    collectd.info('mesos plugin [verbose]: %s' % msg)

collectd.register_config(configure_callback)
collectd.register_read(read_callback)
