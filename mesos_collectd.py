#! /usr/bin/python
#
# Common functions for master and slave plugins.

import collectd
import json
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
    else:
       stats_cur = dict(STATS_MESOS.items() + STATS_MESOS_022.items())

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


def configure_callback(conf, prefix, cluster, instance, host, port, version,
                       url, verboseLogging):
    """Received configuration information"""
    global MESOS_PREFIX
    MESOS_PREFIX = prefix
    global MESOS_CLUSTER
    MESOS_CLUSTER = cluster
    global MESOS_INSTANCE
    MESOS_INSTANCE = instance
    global MESOS_HOST
    MESOS_HOST = host
    global MESOS_PORT
    MESOS_PORT = port
    global MESOS_VERSION
    MESOS_VERSION = version
    global MESOS_URL
    MESOS_URL = url
    global VERBOSE_LOGGING
    VERBOSE_LOGGING = verboseLogging

    for node in conf.children:
        if node.key == 'Host':
            host = node.values[0]
        elif node.key == 'Port':
            port = int(node.values[0])
        elif node.key == 'Verbose':
            verboseLogging = bool(node.values[0])
        elif node.key == 'Version':
            version = node.values[0]
        elif node.key == 'Instance':
            instance = node.values[0]
        elif node.key == 'Cluster':
            cluster = node.values[0]
        else:
            collectd.warning('%s plugin: Unknown config key: %s.' %
                             (prefix, node.key))
            continue

    log_verbose(verboseLogging,
                '%s plugin configured with host = %s, port = %s, verbose '
                'logging = %s, version = %s, instance = %s, cluster = %s' %
                (prefix, host, port, verboseLogging, version, instance,
                 cluster))
    CONFIGS.append({
        'host': host,
        'port': port,
        'mesos_url': "http://" + host + ":" + str(port) + "/metrics/snapshot",
        'verboseLogging': verboseLogging,
        'version': version,
        'instance': instance,
        'cluster': cluster,
    })


def fetch_stats(conf):
    try:
        result = json.load(urllib2.urlopen(conf['mesos_url'], timeout=10))
    except urllib2.URLError, e:
        collectd.error('%s plugin: Error connecting to %s - %r' %
                       (MESOS_PREFIX, conf['mesos_url'], e))
        return None
    parse_stats(conf, result)


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
                    '%s plugin: Value not found for %s' % (MESOS_PREFIX, name))
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
    cluster_dimension = ''
    if conf['cluster']:
        cluster_dimension = '[cluster=%s]' % conf['cluster']
    val.plugin_instance = '%s%s' % (conf['instance'], cluster_dimension)
    # https://github.com/collectd/collectd/issues/716
    val.meta = {'0': True}
    val.dispatch()


def read_callback(is_master, stats_mesos, stats_mesos_019, stats_mesos_020,
                  stats_mesos_021, stats_mesos_022):
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

    for conf in CONFIGS:
        log_verbose(conf['verboseLogging'], 'Read callback called')
        stats = fetch_stats(conf)


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
    collectd.info('%s plugin [verbose]: %s' % (MESOS_PREFIX, msg))
