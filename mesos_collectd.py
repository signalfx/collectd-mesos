#! /usr/bin/python
#
# Common functions for master and slave plugins.

import collectd
import json
import os
import subprocess
import urllib2


CONFIGS = []


# FUNCTION: gets the list of stats based on the version of mesos
def get_stats_string(version):
    if version == "0.19.0" or version == "0.19.1":
        stats_cur = dict(STATS_MESOS.items() + STATS_MESOS_019.items())
    elif version == "0.20.0" or version == "0.20.1":
        stats_cur = dict(STATS_MESOS.items() + STATS_MESOS_020.items())
    elif version == "0.21.0" or version == "0.21.1":
        stats_cur = dict(STATS_MESOS.items() + STATS_MESOS_021.items())
    elif version == "0.22.0" or version == "0.22.1":
        stats_cur = dict(STATS_MESOS.items() + STATS_MESOS_022.items())
    elif version == "1.0.0" or version == "1.0.1":
        stats_cur = dict(STATS_MESOS.items() + STATS_MESOS_100.items())
    else:
        stats_cur = dict(STATS_MESOS.items() + STATS_MESOS_100.items())

    return stats_cur


# FUNCTION: Collect stats from JSON result
def lookup_stat(stat, json, conf):
    val = dig_it_up(json, get_stats_string(conf['version'])[stat].path)

    # Check to make sure we have a valid result
    # dig_it_up returns False if no match found
    if not isinstance(val, bool):
        return val
    else:
        return None


def configure_callback(conf, is_master, prefix, cluster, instance, path, host,
                       port, url, verboseLogging):
    """Received configuration information"""
    global PREFIX
    PREFIX = prefix
    global MESOS_CLUSTER
    MESOS_CLUSTER = cluster
    global MESOS_INSTANCE
    MESOS_INSTANCE = instance
    global MESOS_PATH
    MESOS_PATH = path
    global MESOS_HOST
    MESOS_HOST = host
    global MESOS_PORT
    MESOS_PORT = port
    global MESOS_URL
    MESOS_URL = url
    global VERBOSE_LOGGING
    VERBOSE_LOGGING = verboseLogging
    include_system_health = False
    system_health_url = None

    for node in conf.children:
        if node.key == 'Host':
            host = node.values[0]
        elif node.key == 'Port':
            port = int(node.values[0])
        elif node.key == 'Verbose':
            verboseLogging = bool(node.values[0])
        elif node.key == 'Instance':
            instance = node.values[0]
        elif node.key == 'Cluster':
            cluster = node.values[0]
        elif node.key == 'Path':
            path = node.values[0]
        elif node.key == 'IncludeSystemHealth':
            include_system_health = node.values[0]
        else:
            collectd.warning('%s plugin: Unknown config key: %s.' %
                             (prefix, node.key))
            continue

    global MESOS_VERSION
    binary = '%s/%s' % (path, 'mesos-master' if is_master else 'mesos-slave')
    if os.path.exists(binary):
        # Expected output: mesos <version_string>
        version = subprocess.check_output([binary, '--version'])
        MESOS_VERSION = version.strip().split()[-1]
    else:
        version = get_version_from_api(host, port)
        MESOS_VERSION = version.strip()

    if include_system_health:
        system_health_url = 'http://{0}:1050/system/health/v1'.format(host)
        log_verbose(verboseLogging,
                    '%s plugin to include system health metrics from url %s'
                    % (prefix, system_health_url))

    log_verbose(verboseLogging,
                '%s plugin configured with host = %s, port = %s, verbose '
                'logging = %s, version = %s, instance = %s, cluster = %s, '
                'path = %s' %
                (prefix, host, port, verboseLogging, version, instance,
                 cluster, path))
    CONFIGS.append({
        'host': host,
        'port': port,
        'mesos_url': "http://" + host + ":" + str(port) + "/metrics/snapshot",
        'system_health_url': system_health_url,
        'verboseLogging': verboseLogging,
        'version': version,
        'instance': instance,
        'cluster': cluster,
        'path': path,
    })


def get_version_from_api(host, port):
    version_api_url = "http://" + host + ":" + str(port) + "/version"
    try:
        result = json.load(urllib2.urlopen(version_api_url, timeout=10))
        return result['version']
    except urllib2.URLError, e:
        collectd.error('%s plugin: Error connecting to %s - %r' %
                       (PREFIX, version_api_url, e))
        return None


def fetch_stats(conf):
    try:
        result = json.load(urllib2.urlopen(conf['mesos_url'], timeout=10))
    except urllib2.URLError, e:
        collectd.error('%s plugin: Error connecting to %s - %r' %
                       (PREFIX, conf['mesos_url'], e))
        return None
    parse_stats(conf, result)


def fetch_system_health(conf):
    """Fetch system health metrics"""
    system_health_url = conf['system_health_url']
    if IS_MASTER and system_health_url is not None:
        try:
            result = json.load(urllib2.urlopen(system_health_url, timeout=10))
            for unit in result['units']:
                dims = (',system_component=%s,system_component_name=%s' %
                        (unit['id'], unit['name']))
                dispatch_system_health(unit['health'],
                                       'mesos.service.health',
                                       'gauge', conf, dims)

        except urllib2.URLError, e:
            collectd.error('%s plugin: Error connecting to %s - %r' %
                           (PREFIX, system_health_url, e))
            return None


def dispatch_system_health(metric_value, metric_name, metric_type, conf, dims):
    """Dispatch a system health metric value"""
    if metric_value is None:
        log_verbose(conf['verboseLogging'],
                    '%s plugin: Value not found for %s'
                    % (PREFIX, metric_name))
        return
    log_verbose(conf['verboseLogging'],
                'Sending value[%s]: %s=%s for instance:%s' %
                (metric_type, metric_name, metric_value, conf['instance']))

    val = collectd.Values(plugin='mesos')
    val.type = metric_type
    val.type_instance = metric_name
    val.values = [metric_value]
    plugin_type = 'master'
    cluster_dimension = ''
    if conf['cluster']:
        cluster_dimension = ',cluster=%s' % conf['cluster']
    val.plugin_instance = ('%s[plugin_type=%s%s%s]' %
                           (conf['instance'], plugin_type,
                            cluster_dimension, dims))
    val.meta = {'0': True}
    val.dispatch()


def parse_stats(conf, json):
    """Parse stats response from Mesos"""
    if IS_MASTER:
        # Ignore stats if coming from non-leading mesos master
        elected_result = lookup_stat('master/elected', json, conf)
        if elected_result == 1:
            for name, key in get_stats_string(conf['version']).iteritems():
                result = lookup_stat(name, json, conf)
                dispatch_stat(result, name, key, conf)
        else:
            log_verbose(conf['verboseLogging'],
                        'This mesos master node is not elected leader so not '
                        'writing data.')
            return None
    else:
        for name, key in get_stats_string(conf['version']).iteritems():
            result = lookup_stat(name, json, conf)
            dispatch_stat(result, name, key, conf)


def dispatch_stat(result, name, key, conf):
    """Read a key from info response data and dispatch a value"""
    if result is None:
        log_verbose(conf['verboseLogging'],
                    '%s plugin: Value not found for %s' % (PREFIX, name))
        return
    estype = key.type
    value = result
    log_verbose(conf['verboseLogging'],
                'Sending value[%s]: %s=%s for instance:%s' %
                (estype, name, value, conf['instance']))

    val = collectd.Values(plugin='mesos')
    val.type = estype
    val.type_instance = name
    val.values = [value]
    plugin_type = 'master' if IS_MASTER else 'slave'
    cluster_dimension = ''
    if conf['cluster']:
        cluster_dimension = ',cluster=%s' % conf['cluster']
    val.plugin_instance = ('%s[plugin_type=%s%s]' %
                           (conf['instance'], plugin_type, cluster_dimension))
    # https://github.com/collectd/collectd/issues/716
    val.meta = {'0': True}
    val.dispatch()


def read_callback(is_master, stats_mesos, stats_mesos_019, stats_mesos_020,
                  stats_mesos_021, stats_mesos_022, stats_mesos_100):
    global IS_MASTER
    IS_MASTER = is_master
    global STATS_MESOS
    STATS_MESOS = stats_mesos
    global STATS_MESOS_019
    STATS_MESOS_019 = stats_mesos_019
    global STATS_MESOS_020
    STATS_MESOS_020 = stats_mesos_020
    global STATS_MESOS_021
    STATS_MESOS_021 = stats_mesos_021
    global STATS_MESOS_022
    STATS_MESOS_022 = stats_mesos_022
    global STATS_MESOS_100
    STATS_MESOS_100 = stats_mesos_100

    for conf in CONFIGS:
        log_verbose(conf['verboseLogging'], 'Read callback called')
        stats = fetch_stats(conf)
        if IS_MASTER:
            fetch_system_health(conf)


def dig_it_up(obj, path):
    try:
        if type(path) in (str, unicode):
            path = path.split('.')
        return reduce(lambda x, y: x[y], path, obj)
    except:
        return False


def log_verbose(enabled, msg):
    if not enabled:
        return
    collectd.info('%s plugin [verbose]: %s' % (PREFIX, msg))
