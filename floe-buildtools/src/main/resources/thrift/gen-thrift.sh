#!/bin/bash
#
# Copyright 2014 University of Southern California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

rm -rf ../../../../../floe-core/src/main/java/edu/usc/pgroup/floe/thriftgen
thrift --gen java:beans,hashcode,nocamel ../../../../../floe-core/src/floe.thrift
mv gen-javabean/edu/usc/pgroup/floe/thriftgen ../../../../../floe-core/src/main/java/edu/usc/pgroup/floe/
rm -rf gen-javabean
