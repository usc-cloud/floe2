/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */
namespace java edu.usc.pgroup.floe.thriftgen

enum TTupleArgTypes {
    i32_arg,
    i64_arg,
    string_arg,
    binary_argT
}

enum TChannelType {
    roundrobin,
    //Todo: add other channel types here later.
}

struct TEdge {
    1: required string srcPelletId;
    2: required string destPelletId;
    3: required TChannelType channelType;
}

struct TPellet {
    1: required string id;
    2: required list<TEdge> incomingEdges;
    3: required list<TEdge> outgoingEdges;
    4: required binary serializedPellet;
    5: optional i32 parallelism;
        //2: required list<TTupleArgTypes> outputFieldTypes;
        //3: required list<string> orderedInputFieldNames;
        //4: required map<string, TTupleArgTypes> inputFieldTypes;
}

struct TFloeApp {
    1: required map<string, TPellet> pellets;
    2: optional string jarPath;
}

exception InsufficientResourcesException {
    1: required string msg;
}

exception DuplicateException {
    1: required string msg;
}

exception AppNotFoundException {
    1: required string msg;
}

exception PelletNotFoundException {
    1: required string msg;
}

exception InvalidAppException {
    1: required string msg;
}

enum ScaleDirection {
    up,
    down
}

service TCoordinator
{
    //returns pingText back
    string ping(1: string pingText);

    i32 beginFileUpload(1: string filename);
    void uploadChunk(1: i32 fid, 2: binary chunk);
    void finishUpload(1: i32 fid);

    i32 beginFileDownload(1: string filename);
    binary downloadChunk(1:i32 fid);

    void submitApp(1: string appName, 2: TFloeApp app) throws
    (1: InsufficientResourcesException ire,
    2: DuplicateException de,
    3: InvalidAppException iae);


    void scale(1: ScaleDirection direction, 2: string appName,
    3: string pelletName, 4: i32 count) throws
    (1: InsufficientResourcesException ire,
    2: AppNotFoundException anfe,
    3: PelletNotFoundException pnfe)
}