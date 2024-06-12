set PATH_TO_MONICA_BIN_DIR=C:\MONICA\monica_win64_3.6.16\bin
set PATH_TO_PYTHON=c:\Users\palka\AppData\Local\anaconda3\python.exe
set MONICA_PARAMETERS=%cd%\data\params
echo "MONICA_PARAMETERS=%MONICA_PARAMETERS%"

START "ZMQ_IN_PROXY" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-proxy -pps -f 6666 -b 6677 &
START "ZMQ_OUT_PROXY" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-proxy -pps -f 7788 -b 7777 &

START "ZMQ_MONICA_1" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788
START "ZMQ_MONICA_2" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788
START "ZMQ_MONICA_3" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788
START "ZMQ_MONICA_4" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788
START "ZMQ_MONICA_5" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788

echo "run producer"
START "ZMQ_PRODUCER" /MIN %PATH_TO_PYTHON% run-producer.py

echo "run consumer"
%PATH_TO_PYTHON% run-consumer.py

echo "killing proxies"
taskkill /FI "WindowTitle eq ZMQ_IN_PROXY*" /T /F
taskkill /FI "WindowTitle eq ZMQ_OUT_PROXY*" /T /F

echo "killing MONICAs
taskkill /FI "WindowTitle eq ZMQ_MONICA_*" /T /F