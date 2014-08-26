/*
 * Copyright 2014 University of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** This package is responsible for launching and monitoring external processes
 * . E.g. the Flakes.
 * Local processes are monitored using a file based heartbeat system.
 * NOTE: We initially used an implicit monitoring using the Process class,
 * but that is not easy to be made fault tolerant.
 * @author kumbhare
 */
package edu.usc.pgroup.floe.processes.monitor;
