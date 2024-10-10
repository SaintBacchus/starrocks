#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
# This script is used to build arrow from source.
#
# Arguments:
#    $1 - Base path for logs/artifacts.
#    $2 - type of test (e.g. test or benchmark)
#    $3 - path to executable
#    $ARGN - arguments for executable
#
set -e

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

export STARROCKS_HOME=${STARROCKS_HOME:-$curdir/../..}

pushd $STARROCKS_HOME
git submodule update --init starrocks_format_sdk/src/main/cpp/arrow/
popd

cd $STARROCKS_HOME/starrocks_format_sdk/src/main/cpp/arrow

# build arrow
mkdir -p cpp/build
cd cpp/build
cmake ..  --preset  ninja-release-basic -DCMAKE_C_COMPILER=/opt/rh/gcc-toolset-10/root/usr/bin/gcc -DARROW_BUILD_STATIC=ON -DARROW_JEMALLOC=OFF
cmake --build .

