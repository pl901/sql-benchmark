#!/bin/bash

get_arch=`arch`
if [[ $get_arch =~ "x86_64" ]];then
    ./jre_linux_x64/bin/java -jar sql-benchmark-1.0-SNAPSHOT.jar
elif [[ $get_arch =~ "aarch64" ]];then
    ./jre_linux_aarch64/bin/java -jar sql-benchmark-1.0-SNAPSHOT.jar
else
    echo "unknown cpu architecture!!"
fi

