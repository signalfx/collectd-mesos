#! /usr/bin/python
# Copyright 2015 Ray Rodriguez
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
import collections

import mesos_collectd


IS_MASTER = False
PREFIX = "mesos-slave"
MESOS_CLUSTER = "cluster-0"
MESOS_INSTANCE = "slave-0"
MESOS_PATH = "/usr/sbin"
MESOS_HOST = "localhost"
MESOS_PORT = 5051
MESOS_URL = ""
VERBOSE_LOGGING = False

Stat = collections.namedtuple('Stat', ('type', 'path'))

# DICT: Common Metrics in 0.19.0, 0.20.0, 0.21.0, 0.22.0 and 0.23.0
STATS_MESOS = {
    # Slave
    'slave/frameworks_active': Stat("gauge", "slave/frameworks_active"),
    'slave/invalid_framework_messages':
        Stat("counter", "slave/invalid_framework_messages"),
    'slave/invalid_status_updates':
        Stat("counter", "slave/invalid_status_updates"),
    'slave/recovery_errors': Stat("counter", "slave/recovery_errors"),
    'slave/registered': Stat("gauge", "slave/registered"),
    'slave/tasks_failed': Stat("counter", "slave/tasks_failed"),
    'slave/tasks_finished': Stat("counter", "slave/tasks_finished"),
    'slave/tasks_killed': Stat("counter", "slave/tasks_killed"),
    'slave/tasks_lost': Stat("counter", "slave/tasks_lost"),
    'slave/tasks_running': Stat("gauge", "slave/tasks_running"),
    'slave/tasks_starting': Stat("gauge", "slave/tasks_starting"),
    'slave/tasks_staging': Stat("gauge", "slave/tasks_staging"),
    'slave/uptime_secs': Stat("gauge", "slave/uptime_secs"),
    'slave/valid_framework_messages':
        Stat("counter", "slave/valid_framework_messages"),
    'slave/valid_status_updates':
        Stat("counter", "slave/valid_status_updates"),

    # System
    'system/cpus_total': Stat("gauge", "system/cpus_total"),
    'system/load_15min': Stat("gauge", "system/load_15min"),
    'system/load_1min': Stat("gauge", "system/load_1min"),
    'system/load_5min': Stat("gauge", "system/load_5min"),
    'system/mem_free_bytes': Stat("bytes", "system/mem_free_bytes"),
    'system/mem_total_bytes': Stat("bytes", "system/mem_total_bytes")
}

# DICT: Mesos 0.19.0, 0.19.1
STATS_MESOS_019 = {
}

# DICT: Mesos 0.20.0, 0.20.1
STATS_MESOS_020 = {
    'slave/executors_registering':
        Stat("gauge", "slave/executors_registering"),
    'slave/executors_running': Stat("gauge", "slave/executors_running"),
    'slave/executors_terminating':
        Stat("gauge", "slave/executors_terminating"),
    'slave/executors_terminated': Stat("counter", "slave/executors_terminated")
}

# DICT: Mesos 0.21.0, 0.21.1
STATS_MESOS_021 = {
    'slave/cpus_percent': Stat("percent", "slave/cpus_percent"),
    'slave/cpus_total': Stat("gauge", "slave/cpus_total"),
    'slave/cpus_used': Stat("gauge", "slave/cpus_used"),
    'slave/disk_percent': Stat("percent", "slave/disk_percent"),
    'slave/disk_total': Stat("gauge", "slave/disk_total"),
    'slave/disk_used': Stat("gauge", "slave/disk_used"),
    'slave/mem_percent': Stat("percent", "slave/mem_percent"),
    'slave/mem_total': Stat("gauge", "slave/mem_total"),
    'slave/mem_used': Stat("gauge", "slave/mem_used"),
    'slave/executors_registering':
        Stat("gauge", "slave/executors_registering"),
    'slave/executors_running': Stat("gauge", "slave/executors_running"),
    'slave/executors_terminating':
        Stat("gauge", "slave/executors_terminating"),
    'slave/executors_terminated': Stat("counter", "slave/executors_terminated")
}

# DICT: Mesos 0.22.0, 0.22.1
STATS_MESOS_022 = STATS_MESOS_021.copy()


def configure_callback(conf):
    mesos_collectd.configure_callback(conf, IS_MASTER, PREFIX, MESOS_CLUSTER,
                                      MESOS_INSTANCE, MESOS_PATH, MESOS_HOST,
                                      MESOS_PORT, MESOS_URL, VERBOSE_LOGGING)


def read_callback():
    mesos_collectd.read_callback(IS_MASTER, STATS_MESOS, STATS_MESOS_019,
                                 STATS_MESOS_020, STATS_MESOS_021,
                                 STATS_MESOS_022)


collectd.register_config(configure_callback)
collectd.register_read(read_callback)
