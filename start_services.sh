#!/bin/bash

PATH_TO_MONICA_BIN_DIR=/home/berg/GitHub/monica/_cmake_debug
MONICA_PARAMETERS=$(pwd)/data/monica-parameters
export MONICA_PARAMETERS
echo "$MONICA_PARAMETERS"

$PATH_TO_MONICA_BIN_DIR/mas-infrastructure/common/channel -p9921 -r r_in -w w_in --output_srs &
#in_channel=$!

$PATH_TO_MONICA_BIN_DIR/mas-infrastructure/common/channel -p9922 -r r_out -w w_out --output_srs &
#out_channel=$!

exit 0
#sleep 5

monica_pids=()
for _ in {1..3}
do
  #echo "bla"
  $PATH_TO_MONICA_BIN_DIR/monica-capnp-fbp-component --env_in_sr=capnp://localhost:9921/r_in --result_out_sr=capnp://localhost:9922/w_out &
  monica_pids+=($!)
done
echo "monica_pids -> ${monica_pids[*]}"

poetry run python -m zalfmas_services.soil.sqlite_soil_data_service \
  path_to_sqlite_db=data/germany/buek200.sqlite \
  path_to_ascii_soil_grid=data/germany/buek200_1000_25832_etrs89-utm32n.asc \
  port=9901 grid_crs=utm32n srt=soil &
#soil_pid=$!

#poetry run python -m zalfmas_services.grid.ascii_grid \
#  path_to_ascii_grid=data/germany/buek200_1000_25832_etrs89-utm32n.asc \
#  port=9901 grid_crs=utm32n val_type=int srt=soil &
#soil_pid=$!

poetry run python -m zalfmas_services.grid.ascii_grid \
  path_to_ascii_grid=data/germany/dem_1000_25832_etrs89-utm32n.asc \
  port=9902 grid_crs=utm32n val_type=float srt=dem &
#dem_pid=$!

poetry run python -m zalfmas_services.grid.ascii_grid \
  path_to_ascii_grid=data/germany/slope_1000_25832_etrs89-utm32n.asc \
  port=9903 grid_crs=utm32n val_type=float srt=slope &

#echo "slope finished -> kill all other services"
#kill "$soil_pid"
#kill "$dem_pid"

