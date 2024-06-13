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
import itertools

import capnp
from collections import defaultdict
from datetime import date, timedelta
import json
import os
from pathlib import Path
from pyproj import CRS, Transformer
import sys
import time

import monica_io3
import soil_io
import monica_run_lib as Mrunlib

from zalfmas_common import common
import zalfmas_capnpschemas

sys.path.append(os.path.dirname(zalfmas_capnpschemas.__file__))
import grid_capnp
import soil_capnp
import fbp_capnp
import model_capnp
import common_capnp

PATHS = {
     # adjust the local path to your environment
    "mbm-local-remote": {
        #"include-file-base-path": "/home/berg/GitHub/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/", # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/monica_data/climate-data/", # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/", # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
        "path-to-100-climate-files": "/home/berg/Desktop/marlene/pr_output_csvs/"
    },
    "remoteProducer-remoteMonica": {
        #"include-file-base-path": "/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/data/", # mounted path to archive or hard drive with climate data 
        "monica-path-to-climate-dir": "/monica_data/climate-data/", # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/", # mounted path to archive or hard drive with data 
        "path-debug-write-folder": "/out/debug-out/",
    }
}

TEMPLATE_PATH_LATLON = "{path_to_climate_dir}/latlon-to-rowcol.json"
TEMPLATE_PATH_CLIMATE_CSV = "{gcm}/{rcm}/{scenario}/{ensmem}/{version}/row-{crow}/col-{ccol}.csv"
TEMPLATE_PATH_HARVEST = "{path_to_data_dir}/projects/monica-germany/ILR_SEED_HARVEST_doys_{crop_id}.csv"


async def run_producer(server=None, port=None):
    local_run = True

    config = {
        "mode": "mbm-local-remote",
        "server-port": port if port else "6666",
        "server": server if server else "localhost",
        "start-row": "0", 
        "end-row": "-1", 
        "path_to_dem_grid": "",
        "sim.json": "sim.json",
        "crop.json": "crop.json",
        "site.json": "site.json",
        "setups-file": "sim_setups.csv",
        "run-setups": "[1]",
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    # select paths 
    paths = PATHS[config["mode"]]

    # read setup from csv file
    setups = Mrunlib.read_sim_setups(config["setups-file"])
    run_setups = json.loads(config["run-setups"])
    print("read sim setups: ", config["setups-file"])

    #transforms geospatial coordinates from one coordinate reference system to another
    wgs84_crs = CRS.from_epsg(4326)
    utm32_crs = CRS.from_epsg(25832)
    wgs84_to_utm32_trans = Transformer.from_crs(wgs84_crs, utm32_crs, always_xy=True)

    ilr_seed_harvest_data = defaultdict(lambda: {"interpolate": None, "data": defaultdict(dict),
                                                 "is-winter-crop": None})

    conman = common.ConnectionManager()
    soil_service = await conman.try_connect("capnp://localhost:9901/soil", cast_as=soil_capnp.Service, retry_secs=1)
    dem_grid = await conman.try_connect("capnp://localhost:9902/dem", cast_as=grid_capnp.Grid, retry_secs=1)
    slope_grid = await conman.try_connect("capnp://localhost:9903/slope", cast_as=grid_capnp.Grid, retry_secs=1)
    monica_in = await conman.try_connect("capnp://localhost:9921/w_in", cast_as=fbp_capnp.Channel.Writer, retry_secs=1)

    start_time = time.perf_counter()

    # run calculations for each setup
    for _, setup_id in enumerate(run_setups):

        if setup_id not in setups:
            continue
        start_setup_time = time.perf_counter()      

        setup = setups[setup_id]
        gcm = setup["gcm"]
        rcm = setup["rcm"]
        scenario = setup["scenario"]
        ensmem = setup["ensmem"]
        version = setup["version"]
        crop_id = setup["crop-id"]

        ## extract crop_id from crop-id name that has possible an extenstion
        crop_id_short = crop_id.split('_')[0]

        # add crop id from setup file
        try:
            #read seed/harvest dates for each crop_id
            path_harvest = TEMPLATE_PATH_HARVEST.format(path_to_data_dir=paths["path-to-data-dir"],  crop_id=crop_id_short)
            print("created seed harvest gk5 interpolator and read data: ", path_harvest)
            Mrunlib.create_seed_harvest_geoGrid_interpolator_and_read_data(path_harvest, wgs84_crs, utm32_crs, ilr_seed_harvest_data)
        except IOError:
            path_harvest = TEMPLATE_PATH_HARVEST.format(path_to_data_dir=paths["path-to-data-dir"],  crop_id=crop_id_short)
            print("Couldn't read file:", path_harvest)
            continue

        cdict = {}
        path = TEMPLATE_PATH_LATLON.format(path_to_climate_dir=paths["path-to-climate-dir"] + setup["climate_path_to_latlon_file"] + "/")
        climate_data_interpolator = Mrunlib.create_climate_geoGrid_interpolator_from_json_file(path, wgs84_crs, utm32_crs, cdict)
        print("created climate_data to gk5 interpolator: ", path)

        # read template sim.json 
        with open(setup.get("sim.json", config["sim.json"])) as _:
            sim_json = json.load(_)
        # change start and end date acording to setup
        if setup["start_date"]:
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
        if setup["end_date"]:
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"]) 
        #sim_json["include-file-base-path"] = paths["include-file-base-path"]

        # read template site.json
        with open(setup.get("site.json", config["site.json"])) as _:
            site_json = json.load(_)

        if len(scenario) > 0 and scenario[:3].lower() == "rcp":
            site_json["EnvironmentParameters"]["rcp"] = scenario

        # read template crop.json
        with open(setup.get("crop.json", config["crop.json"])) as _:
            crop_json = json.load(_)

        crop_json["CropParameters"]["__enable_vernalisation_factor_fix__"] = setup["use_vernalisation_fix"] if "use_vernalisation_fix" in setup else False

        # set the current crop used for this run id
        crop_json["cropRotation"][2] = crop_id

        # create environment template from json templates
        env_template = monica_io3.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": ""
        })

        #def gen_all_row_cols():
        #    for s_row in range(0, srows):
        #        if s_row < int(config["start-row"]):
        #            continue
        #        elif int(config["end-row"]) > 0 and s_row > int(config["end-row"]):
        #            break
        #        for s_col in range(0, scols):
        #            sh = yllcorner + (scellsize / 2) + (srows - s_row - 1) * scellsize
        #            sr = xllcorner + (scellsize / 2) + s_col * scellsize
        #            yield sr, sh, None

        def gen_100_files():
            rowcol_to_latlon = {}
            with open(paths["path-to-data-dir"] + "germany/dwd_core_ensemble_rowcol-to-latlon.json") as _:
                for (row, col), (lat, lon) in json.load(_):
                    rowcol_to_latlon[(row, col)] = (lat, lon)

            for root, _, files in os.walk(paths["path-to-100-climate-files"]):
                for file in files:
                    if file.endswith(".csv"):
                        ps = file[:-4].split("_")
                        c_row_col = (int(ps[-2]), int(ps[-1]))
                        if c_row_col in rowcol_to_latlon:
                            c_lat, c_lon = rowcol_to_latlon[c_row_col]
                        else:
                            continue
                        yield c_lat, c_lon, file

        soil_id_cache = {}
        sent_env_count = 0
        #for sr, sh, file_name in gen_all_row_cols():
        for c_lat, c_lon, file_name in itertools.islice(gen_100_files(), 10):

            c_latlon = {"lat": c_lat, "lon": c_lon}
            grid_res = [
                soil_service.closestProfilesAt(coord=c_latlon,
                                               query={
                                                   "mandatory": ["soilType", "sand", "clay", "organicCarbon",
                                                                 "bulkDensity"],
                                                   "optional": ["pH"],
                                               }),  # .profiles,
                dem_grid.closestValueAt(latlonCoord=c_latlon, resolution={"meter": 1000}), #.val,
                slope_grid.closestValueAt(latlonCoord=c_latlon, resolution={"meter": 1000}),#.val
            ]
            soil_profiles_, dem_, slope_ = await asyncio.gather(*grid_res)

            if len(soil_profiles_.profiles) == 0:
            #    env_template["customId"] = {
            #        "setup_id": setup_id,
            #        "clat": round(c_lat, 2), "clon": round(c_lon, 2),
            #        "env_id": sent_env_count,
            #        "nodata": True
            #    }
            #    # print("sent nodata env ", sent_env_count, " customId: ", env_template["customId"])
            #    sent_env_count += 1
                continue
            #else:
            #    env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profiles_.profiles[0]

            worksteps = env_template["cropRotation"][0]["worksteps"]
            sowing_ws = next(filter(lambda ws: ws["type"][-6:] == "Sowing", worksteps))
            harvest_ws = next(filter(lambda ws: ws["type"][-7:] == "Harvest", worksteps))

            sr, sh = wgs84_to_utm32_trans.transform(c_lon, c_lat)
            ilr_interpolate = ilr_seed_harvest_data[crop_id_short]["interpolate"]
            seed_harvest_cs = int(ilr_interpolate(sr, sh)) if ilr_interpolate else None

            # set external seed/harvest dates
            if seed_harvest_cs:
                seed_harvest_data = ilr_seed_harvest_data[crop_id_short]["data"][seed_harvest_cs]
                if seed_harvest_data:
                    is_winter_crop = ilr_seed_harvest_data[crop_id_short]["is-winter-crop"]

                    if setup["sowing-date"] == "fixed":  # fixed indicates that regionally fixed sowing dates will be used
                        sowing_date = seed_harvest_data["sowing-date"]
                    elif setup["sowing-date"] == "auto":  # auto indicates that automatic sowng dates will be used that vary between regions
                        sowing_date = seed_harvest_data["latest-sowing-date"]
                    elif setup["sowing-date"] == "fixed1":  # fixed1 indicates that a fixed sowing date will be used that is the same for entire germany
                        sowing_date = sowing_ws["date"]


                    sds = [int(x) for x in sowing_date.split("-")]
                    sd = date(2001, sds[1], sds[2])
                    sdoy = sd.timetuple().tm_yday

                    if setup["harvest-date"] == "fixed":  # fixed indicates that regionally fixed harvest dates will be used
                        harvest_date = seed_harvest_data["harvest-date"]
                    elif setup["harvest-date"] == "auto":  # auto indicates that automatic harvest dates will be used that vary between regions
                        harvest_date = seed_harvest_data["latest-harvest-date"]
                    elif setup["harvest-date"] == "auto1":  # fixed1 indicates that a fixed harvest date will be used that is the same for entire germany
                        harvest_date = harvest_ws["latest-date"]

                    # print("sowing_date:", sowing_date, "harvest_date:", harvest_date)
                    # print("sowing_date:", sowing_ws["date"], "harvest_date:", sowing_ws["date"])

                    hds = [int(x) for x in harvest_date.split("-")]
                    hd = date(2001, hds[1], hds[2])
                    hdoy = hd.timetuple().tm_yday

                    esds = [int(x) for x in seed_harvest_data["earliest-sowing-date"].split("-")]
                    esd = date(2001, esds[1], esds[2])

                    # sowing after harvest should probably never occur in both fixed setup!
                    if setup["sowing-date"] == "fixed" and setup["harvest-date"] == "fixed":
                        #calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        harvest_ws["date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["date"])

                    elif setup["sowing-date"] == "fixed" and setup["harvest-date"] == "auto":
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["latest-date"])

                    elif setup["sowing-date"] == "fixed" and setup["harvest-date"] == "auto1":
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy - 1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], hds[1], hds[2])
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["latest-date"])

                    elif setup["sowing-date"] == "auto" and setup["harvest-date"] == "fixed":
                        sowing_ws["earliest-date"] = seed_harvest_data["earliest-sowing-date"] if esd > date(esd.year, 6, 20) else "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                        calc_sowing_date = date(2000, 12, 31) + timedelta(days=max(hdoy+1, sdoy))
                        sowing_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(sds[0], calc_sowing_date.month, calc_sowing_date.day)
                        harvest_ws["date"] = seed_harvest_data["harvest-date"]
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["earliest-date"], "<",
                              sowing_ws["latest-date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["date"])

                    elif setup["sowing-date"] == "auto" and setup["harvest-date"] == "auto":
                        sowing_ws["earliest-date"] = seed_harvest_data["earliest-sowing-date"] if esd > date(esd.year, 6, 20) else "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["latest-date"] = seed_harvest_data["latest-sowing-date"]
                        harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["earliest-date"], "<",
                              sowing_ws["latest-date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["latest-date"])

                    elif setup["sowing-date"] == "fixed1" and setup["harvest-date"] == "fixed":
                        #calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        if is_winter_crop:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        else:
                            calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        sowing_ws["date"] = sowing_date
                        # print(seed_harvest_data["sowing-date"])
                        harvest_ws["date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month, calc_harvest_date.day)
                        print("dates: ", int(seed_harvest_cs), ":", sowing_ws["date"])
                        print("dates: ", int(seed_harvest_cs), ":", harvest_ws["date"])

            env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = setup["LeafExtensionModifier"]

            if setup["elevation"] and dem_.val.which() == "f":
                env_template["params"]["siteParameters"]["heightNN"] = dem_.val.f

            if setup["slope"] and slope_.val.which() == "f":
                env_template["params"]["siteParameters"]["slope"] = slope_.val.f / 100.0

            if setup["latitude"]:
                env_template["params"]["siteParameters"]["Latitude"] = c_lat

            if setup["CO2"]:
                env_template["params"]["userEnvironmentParameters"]["AtmosphericCO2"] = float(setup["CO2"])

            if setup["O3"]:
                env_template["params"]["userEnvironmentParameters"]["AtmosphericO3"] = float(setup["O3"])

            if setup["FieldConditionModifier"]:
                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["species"]["FieldConditionModifier"] = float(setup["FieldConditionModifier"])

            if setup["StageTemperatureSum"]:
                stage_ts = setup["StageTemperatureSum"].split('_')
                stage_ts = [int(temp_sum) for temp_sum in stage_ts]
                orig_stage_ts = env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                    "StageTemperatureSum"][0]
                if len(stage_ts) != len(orig_stage_ts):
                    stage_ts = orig_stage_ts
                    print('The provided StageTemperatureSum array is not '
                          'sufficiently long. Falling back to original StageTemperatureSum')

                env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"][
                    "StageTemperatureSum"][0] = stage_ts

            env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = setup["fertilization"]
            env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = setup["irrigation"]

            env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup["NitrogenResponseOn"]
            env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = setup["WaterDeficitResponseOn"]
            env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = setup["EmergenceMoistureControlOn"]
            env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = setup["EmergenceFloodingControlOn"]

            env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]

            if file_name:
                env_template["pathToClimateCSV"] = "/home/berg/GitHub/klimertrag_2/data/germany/col-181.csv"
                #env_template["pathToClimateCSV"] = paths["path-to-100-climate-files"] + "/" + file_name
            else:
                subpath_to_csv = TEMPLATE_PATH_CLIMATE_CSV.format(gcm=gcm, rcm=rcm, scenario=scenario, ensmem=ensmem, version=version, crow=str(crow), ccol=str(ccol))
                for _ in range(4):
                    subpath_to_csv = subpath_to_csv.replace("//", "/")
                env_template["pathToClimateCSV"] = [paths["monica-path-to-climate-dir"] + setup["climate_path_to_csvs"] + "/" + subpath_to_csv]
                if setup["incl_hist"]:

                    if rcm[:3] == "UHO":
                        hist_subpath_to_csv = TEMPLATE_PATH_CLIMATE_CSV.format(gcm=gcm, rcm="CLMcom-CCLM4-8-17", scenario="historical", ensmem=ensmem, version=version, crow=str(crow), ccol=str(ccol))
                        for _ in range(4):
                            hist_subpath_to_csv = hist_subpath_to_csv.replace("//", "/")
                        env_template["pathToClimateCSV"].insert(0, paths["monica-path-to-climate-dir"] + setup["climate_path_to_csvs"] + "/" + hist_subpath_to_csv)

                    elif rcm[:3] == "SMH":
                        hist_subpath_to_csv = TEMPLATE_PATH_CLIMATE_CSV.format(gcm=gcm, rcm="CLMcom-CCLM4-8-17", scenario="historical", ensmem=ensmem, version=version, crow=str(crow), ccol=str(ccol))
                        for _ in range(4):
                            hist_subpath_to_csv = hist_subpath_to_csv.replace("//", "/")
                        env_template["pathToClimateCSV"].insert(0, paths["monica-path-to-climate-dir"] + setup["climate_path_to_csvs"] + "/" + hist_subpath_to_csv)

                    hist_subpath_to_csv = TEMPLATE_PATH_CLIMATE_CSV.format(gcm=gcm, rcm=rcm, scenario="historical", ensmem=ensmem, version=version, crow=str(crow), ccol=str(ccol))
                    for _ in range(4):
                        hist_subpath_to_csv = hist_subpath_to_csv.replace("//", "/")
                    env_template["pathToClimateCSV"].insert(0, paths["monica-path-to-climate-dir"] + setup["climate_path_to_csvs"] + "/" + hist_subpath_to_csv)
            print("pathToClimateCSV:", env_template["pathToClimateCSV"])

            env_template["customId"] = {
                "setup_id": setup_id,
                "clat": round(c_lat, 2), "clon": round(c_lon, 2),
                "env_id": sent_env_count,
                "nodata": False
            }

            capnp_env = model_capnp.Env.new_message()
            # capnp_env.timeSeries = timeseries
            capnp_env.soilProfile = soil_profiles_.profiles[0]
            capnp_env.rest = common_capnp.StructuredText.new_message(value=json.dumps(env_template),
                                                                     structure={"json": None})
            out_ip = fbp_capnp.IP.new_message(content=capnp_env,
                                              attributes=[{"key": "id", "value": common_capnp.Value(ui8=1)}])
            #out_ip = fbp_capnp.IP.new_message(
            #    content=model_capnp.Env.new_message(
            #        rest={"value": json.dumps(env_template), "structure": {"json": None}},
            #        soilProfile=soil_profiles_.profiles[0]))
            await monica_in.write(value=out_ip)
            print("sent env ", sent_env_count, " customId: ", env_template["customId"])
            sent_env_count += 1

        stop_setup_time = time.perf_counter()
        print(f"Sending {sent_env_count} envs for setup {setup_id} took {stop_setup_time - start_setup_time} seconds")


if __name__ == "__main__":
    asyncio.run(capnp.run(run_producer()))
