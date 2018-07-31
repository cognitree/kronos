#!/bin/bash


HOST="localhost"
PORT=8080

MAIN_CLASS=""
APP_NAME=""
APP_HOME="$(cd "`dirname "$0"`"/..; pwd)"
LOG_DIR=$APP_HOME/logs
HEAP_SIZE="128m"

STATUS_CHECK_INTERVAL=3

# required directories
DIR_LIST="$LOG_DIR"

start(){
  echo "starting $APP_NAME"
  is_alive
  if [ $? -eq 1 ];then
    echo "$APP_NAME is already running"
    exit 0
  fi
  java -Xmx$HEAP_SIZE -Xms$HEAP_SIZE -cp "$APP_HOME/conf:$APP_HOME/jars/*" \
  $MAIN_CLASS --host $HOST --port $PORT --resourceBase "$APP_HOME/webapp" --contextPath "/" \
  --descriptor "$APP_HOME/webapp/WEB-INF/web.xml" >> "$LOG_DIR/$APP_NAME-stdout-`date +%Y%m%d`.log" 2>&1 &
  echo "$APP_NAME started: pid[`pgrep -f $MAIN_CLASS`]"
  echo "application logs available at $LOG_DIR/$APP_NAME-stdout-`date +%Y%m%d`.log"
}

stop(){
  echo "stopping $APP_NAME"
  is_alive
  if [ $? -eq 0 ]; then
    echo
    echo "$APP_NAME is already stopped"
    return
  fi
  echo "killing process: pid[`pgrep -f $MAIN_CLASS`]"
  pkill -f $MAIN_CLASS
  echo -n "sent kill signal. waiting for shutdown."
  retry=20
  i=0
  while [ $i -lt $retry ]; do
    is_alive
    if [ $? -eq 0 ]; then
      echo
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
  pid=`pgrep -f $MAIN_CLASS`
  if [ "x" != "x"$pid ]; then
    return 1
  else
    return 0
  fi
}

status(){
  is_alive
  if [ $? -eq 1 ]; then
    echo "$APP_NAME is running... : pid[`pgrep -f $MAIN_CLASS`]"
  else
    echo "$APP_NAME is not running..."
  fi
}

init_dir(){
  for dir in $DIR_LIST; do
    if [ ! -d $dir ];then
      mkdir -v $dir
    fi
  done
}

init_param(){
  case "$1" in
    scheduler)
      APP_NAME="scheduler"
      MAIN_CLASS="com.cognitree.kronos.scheduler.SchedulerApp"
      ;;
    executor)
      APP_NAME="executor"
      MAIN_CLASS="com.cognitree.kronos.executor.ExecutorApp"
      ;;
    *)
      APP_NAME="kronos"
      MAIN_CLASS="com.cognitree.kronos.Application"
  esac
}

# main routine
init_dir
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
    echo $"Usage: $0 {start|stop|status|restart} optional:{scheduler|executor}"
    exit 1
esac