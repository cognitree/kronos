#!/bin/bash

# the script uses pgrep to get back the process id
MAIN_CLASS="com.cognitree.kronos.Application"
APP_NAME=""
MODE=""
APP_HOME="$(cd "`dirname "$0"`"/..; pwd)"
STATUS_CHECK_INTERVAL=3
HOST=${HOST="localhost"}
PORT=${PORT=8080}
HEAP_OPTS=${HEAP_OPTS="-Xmx128m -Xms128m"}
TIMEZONE=${TIMEZONE="UTC"}

start(){
  echo "starting $APP_NAME"
  is_alive
  if [ $? -eq 1 ];then
    echo "$APP_NAME is already running"
    exit 0
  fi

  ARGS=""
  case "$MODE" in
    executor)
      ARGS="--mode $MODE"
      ;;
    scheduler | all)
      ARGS="--mode $MODE --host $HOST --port $PORT --resourceBase $APP_HOME/webapp --contextPath / --descriptor $APP_HOME/webapp/WEB-INF/web.xml"
      ;;
  esac
  java -Duser.timezone=$TIMEZONE $HEAP_OPTS -cp "$APP_HOME/conf:$APP_HOME/lib/*:$APP_HOME/lib/ext/*" $MAIN_CLASS $ARGS
}

stop(){
  echo "stopping $APP_NAME"
  is_alive
  if [ $? -eq 0 ]; then
    echo "$APP_NAME is already stopped"
    return
  fi
  PID=`pgrep -f "mode $MODE"`
  echo "killing process: pid[$PID]"
  kill $PID
  echo -n "sent kill signal. waiting for shutdown."
  retry=20
  i=0
  while [ $i -lt $retry ]; do
    is_alive
    if [ $? -eq 0 ]; then
      echo "$APP_NAME stopped successfully"
      return
    fi
    retry=$(($retry - 1))
    echo -n "."
    sleep $STATUS_CHECK_INTERVAL
  done
  echo
  echo "$APP_NAME is still running!!"
}

is_alive() {
  pid=`pgrep -f "mode $MODE"`
  if [ "x" != "x"$pid ]; then
    return 1
  else
    return 0
  fi
}

status(){
  is_alive
  if [ $? -eq 1 ]; then
    echo "$APP_NAME is running... : pid[`pgrep -f "mode $MODE"`]"
  else
    echo "$APP_NAME is not running..."
  fi
}

init_param(){
  case "$1" in
    scheduler)
      APP_NAME="scheduler"
      MODE="scheduler"
      ;;
    executor)
      APP_NAME="executor"
      MODE="executor"
      ;;
    *)
      APP_NAME="kronos"
      MODE="all"
  esac
}

# main routine
init_param $2

case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  status)
    status
    ;;
  restart)
    stop
    start
    ;;
  *)
    echo $"Usage: $0 {start|stop|status|restart} optional:{scheduler|executor|all}"
    exit 1
esac