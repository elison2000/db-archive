#!/bin/sh
#export GIN_MODE=release

status(){
CNT=`ps -ef |grep "$1"|grep -v grep|wc -l`
if [ $CNT -eq 0 ];then
printf "%-20s [stoped]\n" $1
else
printf "%-20s [running]\n" $1
fi
}

start(){
CNT=`ps -ef |grep "$1"|grep -v grep|wc -l`
if [ $CNT -eq 0 ];then
mv -f running.log running.log.old
nohup ./$1 &>running.log &
printf "%-20s [starting]\n" $1
else
printf "%-20s [running]\n" $1
fi
}

stop(){
CNT=`ps -ef |grep "$1"|grep -v grep|wc -l`
if [ $CNT -eq 1 ];then
    PID=`ps -ef |grep "$1"|grep -v grep|awk '{print $2}'`
    kill $PID
    for i in `seq 60`
    do
        sleep 1
        CNT=`ps -ef |grep "$1"|grep -v grep|wc -l`
        if [ $CNT -eq 0 ];then
        printf "%-20s [stoped]\n" $1
        exit 1
        fi
    done
    printf "%-20s [stopging timeout,you can try:kill -9 $PID]\n" $1
elif [ $CNT -eq 0 ];then
    printf "%-20s [stoped]\n" $1
else
    printf "%-20s [unkown]\n" $1
fi
}


#主程序

case "$1" in
  'status')
    status DBArchive

    ;;
  'start')
    start DBArchive
    status DBArchive
    ;;
  'stop')
    stop DBArchive
    status DBArchive
    ;;
  *)
    echo "Usage: ./admin.sh {start|stop|status}"
    exit 1
    ;;
esac