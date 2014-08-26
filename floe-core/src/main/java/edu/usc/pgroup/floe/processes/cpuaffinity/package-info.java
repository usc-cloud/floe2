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

/**
 * This is an internal package responsible for assigning flake processes to
 * specific processors.
 * This has been built using the concepts in the Java-Thread-Affinity
 * library, applying it to processes instead.
 * See: {https://github.com/OpenHFT/Java-Thread-Affinity}
 * It uses JNA to interact with either th GLibC on Linux or Kernel32 library
 * on windows. (MAC is currently not supported).
 * @author kumbhare
 */
package edu.usc.pgroup.floe.processes.cpuaffinity;
