# ==================================================================================================
# Copyright 2014 Twitter, Inc.
# --------------------------------------------------------------------------------------------------
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this work except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file, or at:
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==================================================================================================


import multiprocessing

from zktraffic.base.process import ProcessOptions
from zktraffic.stats.loaders import QueueStatsLoader
from zktraffic.stats.accumulators import (
  PerAuthStatsAccumulator,
  PerIPStatsAccumulator,
  PerPathStatsAccumulator,
)

from .endpoints_server import EndpointsServer

from twitter.common.http import HttpServer
from twitter.common.http.server import request,response
import json


class StatsServer(EndpointsServer):
  def __init__(self,
               iface,
               zkport,
               aggregation_depth,
               max_results=EndpointsServer.MAX_RESULTS,
               max_reqs=400000,
               max_reps=400000,
               max_events=400000,
               start_sniffer=True,
               timer=None,
               sampling=1.0,
               include_bytes=True):

    # Forcing a load of the multiprocessing module here
    # seem to be hitting http://bugs.python.org/issue8200
    multiprocessing.current_process().name

    self._max_results = max_results

    self._stats = QueueStatsLoader(max_reqs, max_reps, max_events, timer)

    self._stats.register_accumulator(
      'per_path', PerPathStatsAccumulator(aggregation_depth, include_bytes))
    self._stats.register_accumulator(
      'per_ip', PerIPStatsAccumulator(aggregation_depth, include_bytes))
    self._stats.register_accumulator(
      'per_auth', PerAuthStatsAccumulator(aggregation_depth, include_bytes))

    self._stats.start()

    super(StatsServer, self).__init__(
      iface,
      zkport,
      self._stats.handle_request,
      self._stats.handle_reply,
      self._stats.handle_event,
      start_sniffer,
      sampling=sampling)

  def wakeup(self):
    self._stats.wakeup()

  @property
  def has_stats(self):
    return len(self._get_stats('per_path')) > 0

  def _get_stats(self, name, prefix='', output_array=False):
    stats_by_opname = self._stats.stats(name, self._max_results)

    if output_array:
        stats_arr = []
        response.content_type = 'application/json'
    else:
        stats = {}
    for opname, opstats in stats_by_opname.items():
      for path, value in opstats.items():
        if output_array:
            #TODO: split prefix to each own key
            #TODO: when per_ip: split path by ':' and add IPs to their own key
            if prefix.endswith('/'):
                prefix = prefix[:-1]
            tmp_dict = {"opname": opname, "path": "%s%s" % (prefix, path), "value":value}
            stats_arr.append(tmp_dict)
        else:
            stats["%s%s%s" % (prefix, opname, path)] = value
    if output_array:
        stats_json = json.dumps(stats_arr)
        return stats_json
    else:
        return stats

  @HttpServer.route("/json/paths")
  def json_paths(self):
    array_get = HttpServer.request.GET.get("array_out")
    array_output = array_get and array_get.lower() in ['true', '1', 't', 'y', 'yes']
    return self._get_stats('per_path', '', array_output)

  @HttpServer.route("/json/ips")
  def json_ips(self):
    array_get = HttpServer.request.GET.get("array_out")
    array_output = array_get and array_get.lower() in ['true', '1', 't', 'y', 'yes']
    return self._get_stats('per_ip', 'per_ip/', array_output)

  @HttpServer.route("/json/auths")
  def json_auths(self):
    array_get = HttpServer.request.GET.get("array_out")
    array_output = array_get and array_get.lower() in ['true', '1', 't', 'y', 'yes']
    return self._get_stats('per_auth', 'per_auth/', array_output)

  @HttpServer.route("/json/auths-dump")
  def json_auths_dump(self):
    return self._stats.auth_by_client

  @HttpServer.route("/json/info")
  def json_info(self):
    """ general info about this instance """
    proc = ProcessOptions()
    return {
      "uptime": proc.uptime
    }
