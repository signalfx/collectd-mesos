#! /usr/bin/python
#
# Common functions for master and slave plugins.

import json
import os
import ssl
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from functools import reduce

import collectd

CONFIGS = []


# FUNCTION: gets the list of stats based on the version of mesos
def get_stats_string(version):
    if version == "0.19.0" or version == "0.19.1":
        stats_cur = dict(list(STATS_MESOS.items()) + list(STATS_MESOS_019.items()))
    elif version == "0.20.0" or version == "0.20.1":
        stats_cur = dict(list(STATS_MESOS.items()) + list(STATS_MESOS_020.items()))
    elif version == "0.21.0" or version == "0.21.1":
        stats_cur = dict(list(STATS_MESOS.items()) + list(STATS_MESOS_021.items()))
    elif version == "0.22.0" or version == "0.22.1":
        stats_cur = dict(list(STATS_MESOS.items()) + list(STATS_MESOS_022.items()))
    elif version == "1.0.0" or version == "1.0.1":
        stats_cur = dict(list(STATS_MESOS.items()) + list(STATS_MESOS_100.items()))
    else:
        stats_cur = dict(list(STATS_MESOS.items()) + list(STATS_MESOS_022.items()))

    return stats_cur


# FUNCTION: returns a properly formated string of dimensions from a dictionary
def _d(d):
    """Formats a dictionary of key/value pairs as a comma-delimited list of
        key=value tokens."""
    return ",".join(["=".join(p) for p in list(d.items())])


# FUNCTION: gets the list of framework metrics based on the version of mesos
def get_framework_string(version):
    stats_cur = dict(list(FRAMEWORK_MESOS.items()))
    return stats_cur


# FUNCTION: gets the list of task metrics based on the version of mesos
def get_task_string(version):
    stats_cur = dict(list(TASK_MESOS.items()))
    return stats_cur


def get_dimension_string(version):
    dimensions_cur = dict(list(DIMENSIONS_MESOS.items()))
    return dimensions_cur


def lookup_framework_stat(stat, json, conf):
    try:
        val = dig_it_up(json, get_framework_string(conf["version"])[stat].path)
    except KeyError as e:
        val = None
        log_verbose(
            conf.get("verboseLogging", False),
            "Encountered key error {0} while looking up framework stat {1}".format(e, stat),
        )
        val = None
    return val


def lookup_task_stat(stat, json, conf):
    try:
        val = dig_it_up(json, get_task_string(conf["version"])[stat].path)
    except KeyError as e:
        log_verbose(
            conf.get("verboseLogging", False),
            "Encountered key error {0} while looking up task stat {1}".format(e, stat),
        )
        val = None
    return val


# FUNCTION: Collect stats from JSON result
def lookup_stat(stat, json, conf):
    try:
        val = dig_it_up(json, get_stats_string(conf["version"])[stat].path)
    except KeyError as e:
        log_verbose(
            conf.get("verboseLogging", False), "Encountered key error {0} while looking up stats {1}".format(e, stat)
        )
        val = None
    # Check to make sure we have a valid result
    # dig_it_up returns None if no match found
    return val


def configure_callback(conf, is_master, prefix, cluster, instance, path, host, port, url, verboseLogging):
    """Received configuration information"""
    global ELECTED
    ELECTED = 0
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
    scheme = "http"
    dcos_sfx_username = None
    dcos_sfx_password = None
    dcos_url = None
    ca_file_path = None

    for node in conf.children:
        if node.key == "Host":
            host = node.values[0]
        elif node.key == "Port":
            port = int(node.values[0])
        elif node.key == "Verbose":
            verboseLogging = bool(node.values[0])
        elif node.key == "Instance":
            instance = node.values[0]
        elif node.key == "Cluster":
            cluster = node.values[0]
        elif node.key == "Path":
            path = node.values[0]
        elif node.key == "IncludeSystemHealth":
            include_system_health = node.values[0]
        elif node.key == "dcos_sfx_username":
            dcos_sfx_username = node.values[0]
        elif node.key == "dcos_sfx_password":
            dcos_sfx_password = node.values[0]
        elif node.key == "scheme":
            scheme = node.values[0]
        elif node.key == "ca_file_path":
            ca_file_path = node.values[0]
        elif node.key == "dcos_url":
            dcos_url = node.values[0]
        else:
            collectd.warning("%s plugin: Unknown config key: %s." % (prefix, node.key))
            continue

    # Relevant only when monitoring mesos hosting DC/OS in strict mode
    dcos_auth_token = ""
    dcos_auth_header = {}
    if dcos_sfx_username and dcos_sfx_password and scheme == "https":
        collectd.info("Configuring Mesos plugin to operate in DC/OS strict mode.")
        dcos_url = dcos_url or "https://leader.mesos/acs/api/v1/auth/login"
        dcos_auth_token = get_dcos_auth_token(dcos_sfx_username, dcos_sfx_password, host, dcos_url, True)
        dcos_auth_header = {"Authorization": ("token=%s" % (str(dcos_auth_token)))}
    try:
        if ca_file_path:
            ssl_context = ssl.create_default_context(cafile=ca_file_path)
        else:
            ssl_context = ssl._create_unverified_context()
    except (AttributeError, FileNotFoundError) as e:
        collectd.error("Failed to configure ssl context: %s" % e)
        ssl_context = None
    scheme += "://"

    global MESOS_VERSION
    binary = "%s/%s" % (path, "mesos-master" if is_master else "mesos-slave")
    version = None
    try:
        if os.path.exists(binary):
            # Expected output: mesos <version_string>
            version = subprocess.check_output([binary, "--version"])
            MESOS_VERSION = version.strip().split()[-1]
        else:
            version_api_url = scheme + host + ":" + str(port) + "/version"
            version = get_version_from_api(
                version_api_url,
                {"dcos_auth_token": dcos_auth_token, "ssl_context": ssl_context, "dcos_auth_header": dcos_auth_header},
            )
            MESOS_VERSION = version.strip()
    except Exception as e:
        collectd.error("Mesos version not obtained (%s)." % (e))

    if include_system_health:
        system_health_url = "{0}{1}:1050/system/health/v1".format(scheme, host)
        log_verbose(
            verboseLogging, "%s plugin to include system health metrics from url %s" % (prefix, system_health_url)
        )

    log_verbose(
        verboseLogging,
        "%s plugin configured with host = %s, port = %s, verbose "
        "logging = %s, version = %s, instance = %s, cluster = %s, "
        "path = %s" % (prefix, host, port, verboseLogging, version, instance, cluster, path),
    )
    CONFIGS.append(
        {
            "host": host,
            "port": port,
            "mesos_url": scheme + host + ":" + str(port) + "/metrics/snapshot",
            "framework_url": (scheme + host + ":" + str(port) + "/master/frameworks"),
            "task_url": scheme + host + ":" + str(port) + "/master/tasks",
            "system_health_url": system_health_url,
            "verboseLogging": verboseLogging,
            "version": version,
            "instance": instance,
            "cluster": cluster,
            "path": path,
            "scheme": scheme,
            "dcos_sfx_username": dcos_sfx_username,
            "dcos_sfx_password": dcos_sfx_password,
            "dcos_auth_token": dcos_auth_token,
            "dcos_url": dcos_url,
            "dcos_auth_header": dcos_auth_header,
            "ca_file_path": ca_file_path,
            "ssl_context": ssl_context,
        }
    )


def get_dcos_auth_token(uid, password, host, dcos_url, verbose_log):
    try:
        log_verbose(verbose_log, "INFO: Getting DC/OS authentication token.")
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"uid": uid, "password": password})
        if not dcos_url:
            raise KeyError("DC/OS url is not configured.")
        try:
            context = ssl._create_unverified_context()
        except AttributeError:
            context = None
        conf = {"dcos_sfx_username": uid, "dcos_sfx_password": password, "host": host, "dcos_url": dcos_url}
        response = get_json(dcos_url, conf, context, headers, data)
        return response["token"]
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        collectd.error("ERROR: Getting DC/OS authentication token failed: (%s)." % (e))
    except KeyError as e:
        collectd.error("ERROR: Getting DC/OS authentication token failed: (%s)." % (e))


def get_version_from_api(url, conf):
    result = get_json(url, conf, conf["ssl_context"], headers=conf["dcos_auth_header"])
    if result:
        return result["version"]


def fetch_stats(conf):
    result = get_json(conf["mesos_url"], conf, conf["ssl_context"], headers=conf["dcos_auth_header"])
    if result:
        parse_stats(conf, result)


def fetch_framework_stats(conf):
    result = get_json(conf["framework_url"], conf, conf["ssl_context"], headers=conf["dcos_auth_header"])
    if result:
        parse_framework_stats(conf, result)


def fetch_task_stats(conf):
    result = get_json(conf["task_url"], conf, conf["ssl_context"], headers=conf["dcos_auth_header"])
    if result:
        parse_task_stats(conf, result)


def fetch_system_health(conf):
    """Fetch system health metrics"""
    system_health_url = conf["system_health_url"]
    if IS_MASTER and system_health_url:
        result = get_json(system_health_url, conf, conf["ssl_context"], headers=conf["dcos_auth_header"])
        if result:
            for unit in result["units"]:
                dims = ",system_component=%s,system_component_name=%s" % (unit["id"], unit["name"])
                dispatch_system_health(unit["health"], "mesos.service.health", "gauge", conf, dims)


def get_json(url, conf, context, headers={}, data=""):
    """
    Makes the API call and prepares the json to be returned
    """
    response = make_api_call(url, conf, context, headers, data)
    try:
        if response:
            return json.load(response)
    except ValueError as e:
        collectd.error("ERROR: JSON parsing failed: (%s) %s" % (e, url))


def make_api_call(url, conf, context, headers, data):
    try:
        req = urllib.request.Request(url, headers=headers, data=data.encode("utf-8"))
        if context:
            response = urllib.request.urlopen(req, context=context)
        else:
            response = urllib.request.urlopen(req)
        return response
    except urllib.error.HTTPError as e:
        try:
            # 401 errors need to be logged as errors under the following circumstances:
            # i.  Getting the Authentication token fails
            # ii. Plugin is deployed in an environment it doesn't support
            # 401 errors need to be suppressed with log_verbose if it is caused by a timed-out token
            # one way to identify this is using the endpoint - /acs/api/v1/auth/login
            if e.code == 401 and conf.get("dcos_url", None) and url.endswith("/acs/api/v1/auth/login"):
                log_verbose(conf.get("Verbose", False), "INFO: Refreshing DC/OS authentication token.")
                refresh_dcos_auth_token(conf)
            elif e.code == 307 and conf.get("dcos_url", None):
                log_verbose(
                    conf.get("Verbose", False),
                    "INFO: Skipping API call to %s because this master is not the leader (%s)." % (url, e),
                )
            else:
                collectd.error("ERROR: API call failed: (%s) %s" % (e, url))
        except:
            pass
    except urllib.error.URLError as e:
        collectd.error("ERROR: API call failed: (%s) %s" % (e, url))
        raise


def refresh_dcos_auth_token(conf):
    try:
        token = get_dcos_auth_token(
            conf["dcos_sfx_username"],
            conf["dcos_sfx_password"],
            conf["host"],
            conf["dcos_url"],
            conf.get("Verbose", False),
        )
        conf["dcos_auth_token"] = token
        conf["dcos_auth_header"] = {"Authorization": ("token=%s" % (str(token)))}
    except Exception as e:
        collectd.error("Refreshing token failed (%s)." % (e))


def dispatch_system_health(metric_value, metric_name, metric_type, conf, dims):
    """Dispatch a system health metric value"""
    if metric_value is None:
        log_verbose(conf["verboseLogging"], "%s plugin: Value not found for %s" % (PREFIX, metric_name))
        return
    log_verbose(
        conf["verboseLogging"],
        "Sending value[%s]: %s=%s for instance:%s" % (metric_type, metric_name, metric_value, conf["instance"]),
    )

    val = collectd.Values(plugin="mesos")
    val.type = metric_type
    val.type_instance = metric_name
    val.values = [metric_value]
    plugin_type = "master"
    cluster_dimension = ""
    if conf["cluster"]:
        cluster_dimension = ",cluster=%s" % conf["cluster"]
    val.plugin_instance = "%s[plugin_type=%s%s%s]" % (conf["instance"], plugin_type, cluster_dimension, dims)
    val.meta = {"0": True}
    val.dispatch()


def parse_stats(conf, json):
    global ELECTED
    """Parse stats response from Mesos"""
    if IS_MASTER:
        # Ignore stats if coming from non-leading mesos master
        elected_result = lookup_stat("master/elected", json, conf)
        ELECTED = elected_result
        if elected_result == 1:
            for name, key in list(get_stats_string(conf["version"]).items()):
                result = lookup_stat(name, json, conf)
                dispatch_stat(result, name, key, conf)
        else:
            # Always dispatch the election stat from each master
            dispatch_stat(elected_result, "master/elected", get_stats_string(conf["version"])["master/elected"], conf)
            log_verbose(conf["verboseLogging"], "This mesos master node is not elected leader so not " "writing data.")
            return None
    else:
        for name, key in list(get_stats_string(conf["version"]).items()):
            result = lookup_stat(name, json, conf)
            dispatch_stat(result, name, key, conf)


def parse_framework_stats(conf, json):
    """Parse framework stats responses from Mesos"""
    global ELECTED
    if IS_MASTER:
        # Ignore stats if coming from non-leading mesos master
        if ELECTED == 1:
            if "frameworks" in json:
                for framework in json["frameworks"]:
                    dimensions = {}
                    for name, key in list(get_dimension_string(conf["version"])["FRAMEWORK"].items()):
                        result = dig_it_up(framework, key)
                        dimensions[name] = str(result)
                    for name, key in list(get_framework_string(conf["version"]).items()):
                        result = lookup_framework_stat(name, framework, conf)

                        # Patch for framework.is_active
                        if name == "framework.is_active":
                            result = int(result)
                        dispatch_stat(result, name, key, conf, dimensions)
            else:
                log_verbose(conf["verboseLogging"], "No framework data was returned by the api")
        else:
            log_verbose(
                conf["verboseLogging"], "This mesos master node is not elected leader so not " "writing framework data."
            )


def parse_task_stats(conf, json):
    """Parse task stats responses from Mesos"""
    global ELECTED
    state_mapping = {
        "TASK_RUNNING": 0,
        "TASK_FINISHED": 1,
        "TASK_STAGING": 2,
        "TASK_STARTING": 3,
        "TASK_KILLING": 4,
        "TASK_LOST": 5,
        "TASK_KILLED": 6,
        "TASK_FAILED": 7,
        "TASK_ERROR": 8,
    }
    if IS_MASTER:
        # Ignore stats if coming from non-leading mesos master
        if ELECTED == 1:
            if "tasks" in json:
                for task in json["tasks"]:
                    dimensions = {}
                    for name, key in list(get_dimension_string(conf["version"])["TASK"].items()):
                        result = dig_it_up(task, key)
                        dimensions[name] = str(result)
                    for name, key in list(get_task_string(conf["version"]).items()):
                        result = lookup_task_stat(name, task, conf)

                        # Patch for task.state
                        if name == "task.state":
                            if result in state_mapping:
                                result = state_mapping[result]
                            else:
                                result = 9
                        dispatch_stat(result, name, key, conf, dimensions)
            else:
                log_verbose(conf["verboseLogging"], "No task data was returned by the api")
        else:
            log_verbose(
                conf["verboseLogging"], "This mesos master node is not elected leader so not " "writing task data."
            )


def dispatch_stat(result, name, key, conf, dimensions=None):
    """Read a key from info response data and dispatch a value"""
    if result is None:
        log_verbose(conf["verboseLogging"], "%s plugin: Value not found for %s" % (PREFIX, name))
        return
    if dimensions is None:
        dimensions = {}
    estype = key.type
    value = result
    log_verbose(
        conf["verboseLogging"], "Sending value[%s]: %s=%s for instance:%s" % (estype, name, value, conf["instance"])
    )
    val = collectd.Values(plugin="mesos")
    val.type = estype
    val.type_instance = name
    val.values = [value]
    dimensions["plugin_type"] = "master" if IS_MASTER else "slave"
    dimensions["cluster"] = ""
    if conf["cluster"]:
        dimensions["cluster"] = conf["cluster"]
    val.plugin_instance = "{instance}[{dims}]".format(instance=conf["instance"], dims=_d(dimensions))
    # https://github.com/collectd/collectd/issues/716
    val.meta = {"0": True}
    val.dispatch()


def read_callback(
    is_master,
    stats_mesos,
    stats_mesos_019,
    stats_mesos_020,
    stats_mesos_021,
    stats_mesos_022,
    stats_mesos_100,
    framework_mesos=None,
    task_mesos=None,
    dimensions_mesos=None,
):
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
    global FRAMEWORK_MESOS
    FRAMEWORK_MESOS = framework_mesos
    global TASK_MESOS
    TASK_MESOS = task_mesos
    global DIMENSIONS_MESOS
    DIMENSIONS_MESOS = dimensions_mesos

    for conf in CONFIGS:
        try:
            log_verbose(conf["verboseLogging"], "Read callback called")
            fetch_stats(conf)
            if IS_MASTER:
                fetch_system_health(conf)
                fetch_framework_stats(conf)
                fetch_task_stats(conf)
        except Exception as e:
            collectd.error("Error fetching metrics: %s" % e)


def dig_it_up(obj, path):
    try:
        if isinstance(path, (str, bytes)):
            path = path.split(".")
        return reduce(lambda x, y: x[y], path, obj)
    except:
        return None


def log_verbose(enabled, msg):
    if not enabled:
        return
    collectd.info("%s plugin [verbose]: %s" % (PREFIX, msg))
