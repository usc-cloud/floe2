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
    ROUND_ROBIN,
    REDUCE,
    LOAD_BALANCED,
    CUSTOM
}

struct TChannel {
    1: required TChannelType channelType;
    2: required string dispersionClass;
    3: required string localDispersionClass;
    4: optional string channelArgs;
}

struct TEdge {
    1: required string srcPelletId;
    2: required string destPelletId;
    3: required TChannel channel;
}

struct TAlternate {
    1: required binary serializedPellet;
    2: required double value;
}

struct TPellet {
    1: required string id;
    2: required list<TEdge> incomingEdges;
    3: required list<string> outputStreamNames;
    4: required map<TEdge, list<string>> outgoingEdgesWithSubscribedStreams;
    5: required map<string, TAlternate> alternates;
    6: required string activeAlternate;
    7: optional i32 parallelism;

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

exception AlternateNotFoundException {
    1: required string msg;
}

exception InvalidAppException {
    1: required string msg;
}

enum ScaleDirection {
    up,
    down
}

/**
 * Enum for App Status.
 */
enum AppStatus {
    /**
     * Application status indicating that this is a new application
     * request and the request has been received.
     */
    NEW_DEPLOYMENT_REQ_RECEIVED,
    /**
     * Application status indicating that currently the application is
     * being scheduled.
     */
    SCHEDULING,
    /**
     * Status to indicate that the scheduling has been completed.
     */
    SCHEDULING_COMPLETED,
    /**
     * Application status indicating that the application has been
     * scheduled and the corresponding flakes are being deployed. Same
     * status is used when the flakes are being updated.
     * This involves launching flakes, establishing socket connections.
     */
    UPDATING_FLAKES,
    /**
     * Status indicating that all flake updates have been completed.
     */
    UPDATING_FLAKES_COMPLETED,
    /**
     * Application status indicating that the flakes has been deployed
     * and the corresponding pellets are being created.
     * THIS SHOULD NOT START THE PELLETS. JUST CREATE THEM.
     * when creating, removing pellets. or changing alternates.
     */
    UPDATING_PELLETS,
    /**
     * Status indicating that the all pellet creation/deletion/updates have
     * been completed.
     */
    UPDATING_PELLETS_COMPLETED,
    /**
     * Status indicating that the application is ready to be started.
     */
    READY,          //READY to start.
    /**
     * Status indicating that the application is running. And no update
     * requests are pending.
     * New requests will be accepted only when in this state.
     */
    RUNNING,
    /**
     * Status to indicate that a new update request has been received.
     */
    REDEPLOYMENT_REQ_RECEIVED,
    /**
     * Status to indicate that the system is Starting pellets.
     */
    STARTING_PELLETS,

    TERMINATED
}

struct TSignal {
    1: required string destApp;
    2: required string destPellet;
    3: required binary data;
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
    3: PelletNotFoundException pnfe);

    void signal(1: TSignal signal) throws
      (1: AppNotFoundException anfe, 2: PelletNotFoundException pnfe);

    void switchAlternate(1: string appName, 2: string pelletName,
          3: string alternateName) throws (1: AppNotFoundException anfe,
          2: PelletNotFoundException pnfe, 3: AlternateNotFoundException alnfe);

    void killApp(1: string appName);

    AppStatus getAppStatus(1: string appName)
      throws (1: AppNotFoundException anfe);
}