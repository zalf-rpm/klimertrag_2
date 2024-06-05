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

from collections import defaultdict
import copy
import csv
from datetime import date, timedelta
import json
import math
import numpy as np
import os
from pyproj import CRS, Transformer
import sqlite3
import sqlite3 as cas_sq3
import sys
import time
import zmq

import monica_io3
import soil_io3
import monica_run_lib as Mrunlib

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

DATA_SOIL_DB = "germany/buek200.sqlite"
DATA_GRID_HEIGHT = "germany/dem_1000_25832_etrs89-utm32n.asc" 
DATA_GRID_SLOPE = "germany/slope_1000_25832_etrs89-utm32n.asc"
DATA_GRID_LAND_USE = "germany/landuse_1000_31469_gk5.asc"
DATA_GRID_SOIL = "germany/buek200_1000_25832_etrs89-utm32n.asc"
# DATA_GRID_CROPS = "germany/crops-all2017-2019_1000_25832_etrs89-utm32n.asc"
# DATA_GRID_CROPS = "germany/dwd-stations-pheno_1000_25832_etrs89-utm32n.asc"
DATA_GRID_CROPS = "germany/germany-complete_1000_25832_etrs89-utm32n.asc"
TEMPLATE_PATH_LATLON = "{path_to_climate_dir}/latlon-to-rowcol.json"
TEMPLATE_PATH_CLIMATE_CSV = "{gcm}/{rcm}/{scenario}/{ensmem}/{version}/row-{crow}/col-{ccol}.csv"

TEMPLATE_PATH_HARVEST = "{path_to_data_dir}/projects/monica-germany/ILR_SEED_HARVEST_doys_{crop_id}.csv"

DEBUG_DONOT_SEND = False
DEBUG_WRITE = False
DEBUG_ROWS = 10
DEBUG_WRITE_FOLDER = "./debug_out"
DEBUG_WRITE_CLIMATE = False

# commandline parameters e.g "server=localhost port=6666 shared_id=2"
def run_producer(server = {"server": None, "port": None}, shared_id = None):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH) # pylint: disable=no-member

    config = {
        "mode": "mbm-local-remote", ## local:"cj-local-remote" remote "mbm-local-remote"
        "server-port": server["port"] if server["port"] else "6666", ## local: 6667, remote 6666
        "server": server["server"] if server["server"] else "localhost",  # "login01.cluster.zalf.de",
        "start-row": "0", 
        "end-row": "-1", 
        "path_to_dem_grid": "",
        "sim.json": "sim.json",
        "crop.json": "crop.json",
        "site.json": "site.json",
        "setups-file": "sim_setups.csv",
        "run-setups": "[1]",
        "shared_id": shared_id
    }
    
    # read commandline args only if script is invoked directly from commandline
    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v

    print("config:", config)

    # select paths 
    paths = PATHS[config["mode"]]
    # open soil db connection
    soil_db_con = sqlite3.connect(paths["path-to-data-dir"] + DATA_SOIL_DB)
    socket.connect("tcp://" + config["server"] + ":" + str(config["server-port"]))

    # read setup from csv file
    setups = Mrunlib.read_sim_setups(config["setups-file"])
    run_setups = json.loads(config["run-setups"])
    print("read sim setups: ", config["setups-file"])

    #transforms geospatial coordinates from one coordinate reference system to another
    # transform wgs84 into gk5
    soil_crs_to_x_transformers = {}
    wgs84_crs = CRS.from_epsg(4326)
    utm32_crs = CRS.from_epsg(25832)
    #transformers[wgs84] = Transformer.from_crs(wgs84_crs, gk5_crs, always_xy=True)

    ilr_seed_harvest_data = defaultdict(lambda: {"interpolate": None, "data": defaultdict(dict), "is-winter-crop": None})

    # Load grids

    # soil data
    path_to_soil_grid = paths["path-to-data-dir"] + DATA_GRID_SOIL
    soil_epsg_code = int(path_to_soil_grid.split("/")[-1].split("_")[2])
    soil_crs = CRS.from_epsg(soil_epsg_code)
    if wgs84_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[wgs84_crs] = Transformer.from_crs(soil_crs, wgs84_crs)
    soil_metadata, _ = Mrunlib.read_header(path_to_soil_grid)
    soil_grid = np.loadtxt(path_to_soil_grid, dtype=int, skiprows=6)
    soil_interpolate = Mrunlib.create_ascii_grid_interpolator(soil_grid, soil_metadata)
    print("read: ", path_to_soil_grid)

    # height data for germany
    path_to_dem_grid = paths["path-to-data-dir"] + DATA_GRID_HEIGHT 
    dem_epsg_code = int(path_to_dem_grid.split("/")[-1].split("_")[2])
    dem_crs = CRS.from_epsg(dem_epsg_code)
    if dem_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[dem_crs] = Transformer.from_crs(soil_crs, dem_crs)
    dem_metadata, _ = Mrunlib.read_header(path_to_dem_grid)
    dem_grid = np.loadtxt(path_to_dem_grid, dtype=float, skiprows=6)
    dem_interpolate = Mrunlib.create_ascii_grid_interpolator(dem_grid, dem_metadata)
    print("read: ", path_to_dem_grid)

    # slope data
    path_to_slope_grid = paths["path-to-data-dir"] + DATA_GRID_SLOPE
    slope_epsg_code = int(path_to_slope_grid.split("/")[-1].split("_")[2])
    slope_crs = CRS.from_epsg(slope_epsg_code)
    if slope_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[slope_crs] = Transformer.from_crs(soil_crs, slope_crs)
    slope_metadata, _ = Mrunlib.read_header(path_to_slope_grid)
    slope_grid = np.loadtxt(path_to_slope_grid, dtype=float, skiprows=6)
    slope_interpolate = Mrunlib.create_ascii_grid_interpolator(slope_grid, slope_metadata)
    print("read: ", path_to_slope_grid)

    # land use data
    path_to_landuse_grid = paths["path-to-data-dir"] + DATA_GRID_LAND_USE
    landuse_epsg_code = int(path_to_landuse_grid.split("/")[-1].split("_")[2])
    landuse_crs = CRS.from_epsg(landuse_epsg_code)
    if landuse_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[landuse_crs] = Transformer.from_crs(soil_crs, landuse_crs)
    landuse_meta, _ = Mrunlib.read_header(path_to_landuse_grid)
    landuse_grid = np.loadtxt(path_to_landuse_grid, dtype=int, skiprows=6)
    landuse_interpolate = Mrunlib.create_ascii_grid_interpolator(landuse_grid, landuse_meta)
    print("read: ", path_to_landuse_grid)

    # crop mask data
    path_to_crop_grid = paths["path-to-data-dir"] + DATA_GRID_CROPS
    crop_epsg_code = int(path_to_crop_grid.split("/")[-1].split("_")[2])
    crop_crs = CRS.from_epsg(crop_epsg_code)
    if crop_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[crop_crs] = Transformer.from_crs(soil_crs, crop_crs)
    crop_meta, _ = Mrunlib.read_header(path_to_crop_grid)
    crop_grid = np.loadtxt(path_to_crop_grid, dtype=int, skiprows=6)
    crop_interpolate = Mrunlib.create_ascii_grid_interpolator(crop_grid, crop_meta)
    print("read: ", path_to_crop_grid)

    sent_env_count = 1
    start_time = time.perf_counter()

    listOfClimateFiles = set()
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
        climate_data_interpolator = Mrunlib.create_climate_geoGrid_interpolator_from_json_file(path, wgs84_crs, soil_crs, cdict)
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

        scols = int(soil_metadata["ncols"])
        srows = int(soil_metadata["nrows"])
        scellsize = int(soil_metadata["cellsize"])
        xllcorner = int(soil_metadata["xllcorner"])
        yllcorner = int(soil_metadata["yllcorner"])
        nodata_value = int(soil_metadata["nodata_value"])

        def gen_all_row_cols():
            for s_row in range(0, srows):
                if s_row < int(config["start-row"]):
                    continue
                elif int(config["end-row"]) > 0 and s_row > int(config["end-row"]):
                    break
                for s_col in range(0, scols):
                    sh = yllcorner + (scellsize / 2) + (srows - s_row - 1) * scellsize
                    sr = xllcorner + (scellsize / 2) + s_col * scellsize
                    yield sr, sh, None

        def gen_100_files():
            rowcol_to_latlon = {}
            with open(paths["path-to-data-dir"] + "germany/dwd_core_ensemble_rowcol-to-latlon.json") as _:
                for (row, col), (lat, lon) in json.load(_):
                    rowcol_to_latlon[(row, col)] = (lat, lon)

            trans = Transformer.from_crs(wgs84_crs, soil_crs, always_xy=True)
            for root, _, files in os.walk(paths["path-to-100-climate-files"]):
                for file in files:
                    if file.endswith(".csv"):
                        ps = file[:-4].split("_")
                        c_row_col = (int(ps[-2]), int(ps[-1]))
                        if c_row_col in rowcol_to_latlon:
                            c_lat, c_lon = rowcol_to_latlon[c_row_col]
                        else:
                            continue
                        sr, sh = trans.transform(c_lon, c_lat)
                        yield sr, sh, file

        soil_id_cache = {}
        sent_env_count = 0
        #for sr, sh, file_name in gen_all_row_cols():
        for sr, sh, file_name in gen_100_files():

            soil_id = int(soil_interpolate(sr, sh))
            if soil_id == nodata_value:
                continue

            #get coordinate of clostest climate element of real soil-cell
            crow, ccol = climate_data_interpolator(sr, sh)

            crop_grid_id = int(crop_interpolate(sr, sh))
            # print(crop_grid_id)
            if crop_grid_id != 1:
                # print("row/col:", srow, "/", scol, "is not a crop pixel.")
                env_template["customId"] = {
                    "setup_id": setup_id,
                    "crow": int(crow), "ccol": int(ccol),
                    "soil_id": soil_id,
                    "env_id": sent_env_count,
                    "nodata": True
                }
                if not DEBUG_DONOT_SEND:
                    socket.send_json(env_template)
                    # print("sent nodata env ", sent_env_count, " customId: ", env_template["customId"])
                    sent_env_count += 1
                continue

            tcoords = {}

            if soil_id in soil_id_cache:
                soil_profile = soil_id_cache[soil_id]
            else:
                soil_profile = soil_io3.soil_parameters(soil_db_con, soil_id)
                soil_id_cache[soil_id] = soil_profile

            if len(soil_profile) == 0:
                env_template["customId"] = {
                    "setup_id": setup_id,
                    "crow": int(crow), "ccol": int(ccol),
                    "soil_id": soil_id,
                    "env_id": sent_env_count,
                    "nodata": True
                }
                if not DEBUG_DONOT_SEND:
                    socket.send_json(env_template)
                    # print("sent nodata env ", sent_env_count, " customId: ", env_template["customId"])
                    sent_env_count += 1
                continue

            worksteps = env_template["cropRotation"][0]["worksteps"]
            sowing_ws = next(filter(lambda ws: ws["type"][-6:] == "Sowing", worksteps))
            harvest_ws = next(filter(lambda ws: ws["type"][-7:] == "Harvest", worksteps))

            ilr_interpolate = ilr_seed_harvest_data[crop_id_short]["interpolate"]
            seed_harvest_cs = ilr_interpolate(sr, sh) if ilr_interpolate else None

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




            # check if current grid cell is used for agriculture
            if setup["landcover"]:
                if landuse_crs not in tcoords:
                    tcoords[landuse_crs] = soil_crs_to_x_transformers[landuse_crs].transform(sr, sh)
                lur, luh = tcoords[landuse_crs]
                landuse_id = landuse_interpolate(lur, luh)
                if landuse_id not in [2,3,4]:
                    continue

            if dem_crs not in tcoords:
                tcoords[dem_crs] = soil_crs_to_x_transformers[dem_crs].transform(sr, sh)
            demr, demh = tcoords[dem_crs]
            height_nn = dem_interpolate(demr, demh)

            if slope_crs not in tcoords:
                tcoords[slope_crs] = soil_crs_to_x_transformers[slope_crs].transform(sr, sh)
            slr, slh = tcoords[slope_crs]
            slope = slope_interpolate(slr, slh)

            env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = setup["LeafExtensionModifier"]

            #print("soil:", soil_profile)
            env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile

            # setting groundwater level
            if setup["groundwater-level"]:
                groundwaterlevel = 20
                layer_depth = 0
                for layer in soil_profile:
                    if layer.get("is_in_groundwater", False):
                        groundwaterlevel = layer_depth
                        #print("setting groundwaterlevel of soil_id:", str(soil_id), "to", groundwaterlevel, "m")
                        break
                    layer_depth += Mrunlib.get_value(layer["Thickness"])
                env_template["params"]["userEnvironmentParameters"]["MinGroundwaterDepthMonth"] = 3
                env_template["params"]["userEnvironmentParameters"]["MinGroundwaterDepth"] = [max(0, groundwaterlevel - 0.2) , "m"]
                env_template["params"]["userEnvironmentParameters"]["MaxGroundwaterDepth"] = [groundwaterlevel + 0.2, "m"]

            # setting impenetrable layer
            if setup["impenetrable-layer"]:
                impenetrable_layer_depth = Mrunlib.get_value(env_template["params"]["userEnvironmentParameters"]["LeachingDepth"])
                layer_depth = 0
                for layer in soil_profile:
                    if layer.get("is_impenetrable", False):
                        impenetrable_layer_depth = layer_depth
                        #print("setting leaching depth of soil_id:", str(soil_id), "to", impenetrable_layer_depth, "m")
                        break
                    layer_depth += Mrunlib.get_value(layer["Thickness"])
                env_template["params"]["userEnvironmentParameters"]["LeachingDepth"] = [impenetrable_layer_depth, "m"]
                env_template["params"]["siteParameters"]["ImpenetrableLayerDepth"] = [impenetrable_layer_depth, "m"]

            if setup["elevation"]:
                env_template["params"]["siteParameters"]["heightNN"] = float(height_nn)

            if setup["slope"]:
                env_template["params"]["siteParameters"]["slope"] = slope / 100.0

            if setup["latitude"]:
                clat, _ = cdict[(crow, ccol)]
                env_template["params"]["siteParameters"]["Latitude"] = clat

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
                env_template["pathToClimateCSV"] = paths["path-to-100-climate-files"] + "/" + file_name
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
                "crow": int(crow), "ccol": int(ccol),
                "soil_id": soil_id,
                "env_id": sent_env_count,
                "nodata": False
            }

            if not DEBUG_DONOT_SEND :
                socket.send_json(env_template)
                print("sent env ", sent_env_count, " customId: ", env_template["customId"])

            sent_env_count += 1

            # write debug output, as json file
            if DEBUG_WRITE:
                debug_write_folder = paths["path-debug-write-folder"]
                if not os.path.exists(debug_write_folder):
                    os.makedirs(debug_write_folder)
                if sent_env_count < DEBUG_ROWS:

                    path_to_debug_file = debug_write_folder + "/row_" + str(sent_env_count-1) + "_" + str(setup_id) + ".json"

                    if not os.path.isfile(path_to_debug_file):
                        with open(path_to_debug_file, "w") as _ :
                            _.write(json.dumps(env_template))
                    else:
                        print("WARNING: Row ", (sent_env_count-1), " already exists")
            #print("unknown_soil_ids:", unknown_soil_ids)

            #print("crows/cols:", crows_cols)
        #cs__.close()
        stop_setup_time = time.perf_counter()
        print("Setup ", (sent_env_count-1), " envs took ", (stop_setup_time - start_setup_time), " seconds")

    stop_time = time.perf_counter()

    try:
        print("sending ", (sent_env_count-1), " envs took ", (stop_time - start_time), " seconds")
        #print("ran from ", start, "/", row_cols[start], " to ", end, "/", row_cols[end]
        print("exiting run_producer()")
    except Exception:
        raise

if __name__ == "__main__":
    run_producer()