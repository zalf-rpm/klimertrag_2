#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import asyncio
import capnp
import csv
import json
import os
import sys

from zalfmas_common import common
from zalfmas_common.model import monica_io
import zalfmas_capnpschemas

sys.path.append(os.path.dirname(zalfmas_capnpschemas.__file__))
import fbp_capnp
import common_capnp

PATHS = {
    "mbm-local-remote": {
        "path-to-data-dir": "data/",
        "path-to-output-dir": "out/",
        "path-to-csv-output-dir": "csv-out/"
    },
    "remoteConsumer-remoteMonica": {
        "path-to-data-dir": "./data/",
        "path-to-output-dir": "/out/out/",
        "path-to-csv-output-dir": "/out/csv-out/"
    }
}


async def run_consumer(leave_after_finished_run=True, server=None, port=None):
    """collect data from workers"""

    config = {
        "mode": "mbm-local-remote",
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    paths = PATHS[config["mode"]]

    if "out" not in config:
        config["out"] = paths["path-to-output-dir"]

    conman = common.ConnectionManager()
    monica_out = await conman.try_connect("capnp://localhost:9922/r_out", cast_as=fbp_capnp.Channel.Reader,
                                          retry_secs=1)
    received_msgs = 0
    while True:
        in_ip = await monica_out.read()
        if in_ip.which() == "done":
            break

        st = in_ip.value.as_struct(fbp_capnp.IP).content.as_text() #struct(common_capnp.StructuredText)
        msg = json.loads(st)

        print(f"received msg: {received_msgs} customId: {msg['customId']}")

        if len(msg["errors"]) > 0:
            print("There were errors in message:", msg["errors"], "\nSkipping message!")
            continue

        custom_id = msg["customId"]
        setup_id = custom_id["setup_id"]
        c_lat = custom_id.get("clat", -1)
        c_lon = custom_id.get("clon", -1)

        path_to_out_dir = config["out"] + str(setup_id)
        print(path_to_out_dir)
        if not os.path.exists(path_to_out_dir):
            try:
                os.makedirs(path_to_out_dir)
            except OSError:
                print("c: Couldn't create dir:", path_to_out_dir, "! Exiting.")
                exit(1)

        with open(f"{path_to_out_dir}/clat-{c_lat}_clon-{c_lon}.csv", "w", newline='') as _:
            writer = csv.writer(_, delimiter=",")
            for data_ in msg.get("data", []):
                results = data_.get("results", [])
                orig_spec = data_.get("origSpec", "")
                output_ids = data_.get("outputIds", [])

                if len(results) > 0:
                    writer.writerow([orig_spec.replace("\"", "")])
                    for row in monica_io.write_output_header_rows(output_ids,
                                                                   include_header_row=True,
                                                                   include_units_row=True,
                                                                   include_time_agg=False):
                        writer.writerow(row)
                    for row in monica_io.write_output_obj(output_ids, results):
                        writer.writerow(row)
                writer.writerow([])

        received_msgs += 1

    print("exiting run_consumer()")

if __name__ == "__main__":
    asyncio.run(capnp.run(run_consumer()))


