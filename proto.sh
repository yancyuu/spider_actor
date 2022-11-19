#!/bin/bash
ipath="."

function walk() {
    include_path=("${ipath}/core")
    for d in `find ${1}`
    do
        if [[ $d = $1 ]];then
            continue
        elif [[ -f $d ]];then
            if [[ ${d##*.} = "proto" ]];then
                c="protoc $d --python_out=. -I${ipath}"
                for inc_path in ${include_path[@]};do
                    if [ -d $inc_path ];then
                        c="$c -I${inc_path}"
                    fi
                done
                `$c`
                if [ $? -eq 0 ];then
                    echo "$c ==> $?"
                fi

            fi
        elif [[ -d $d ]];then
            walk $d
        fi
    done
}

function walk_path() {
    if [ -d $1 ];then
        walk $1
    fi
}

paths=("./proto" "./core/proto" "./commodity" "./common" "./dishes")

for path in ${paths[@]};
do
    walk_path $path
done
