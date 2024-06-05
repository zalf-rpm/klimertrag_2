set PATH_TO_MONICA_BIN_DIR=c:\Users\giri\Documents\monica_win64_3.6.13\bin
set MONICA_PARAMETERS=%cd%\data\monica-parameters
echo "MONICA_PARAMETERS=%MONICA_PARAMETERS%"
START "MONICA" %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -bi -i tcp://localhost:6666 -bo -o tcp://localhost:7777
