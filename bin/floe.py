#!/usr/bin/python

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

import os
import sys
import random
import subprocess as sub
import getopt
import re


FLOE_HOME = "/".join(os.path.realpath( __file__ ).split("/")[:-2])
CONF_DIR = FLOE_HOME + "/conf"
CONFFILE = CONF_DIR + "/config.xml"
JAR_JVM_OPTS = os.getenv('FLOE_JAR_JVM_OPTS', '')
LIB_DIR = os.getenv('FLOE_LIB_DIR', FLOE_HOME + '/floe-core/target/lib')
FLOE_LIB =  os.getenv('FLOE_LIB', FLOE_HOME + '/floe-core/target/floe-core-0.1-SNAPSHOT.jar')
CONFIG_OVERRIDES = []
def get_jars_full(adir):
    files = os.listdir(adir)
    ret = []
    for f in files:
        if f.endswith(".jar"):
            ret.append(adir + "/" + f)
    return ret

def get_classpath(extrajars):
    ret = get_jars_full(FLOE_HOME)
    ret.extend([FLOE_LIB])
    ret.extend(get_jars_full(LIB_DIR))
    ret.extend(extrajars)
    return ":".join(ret)

def parse_args(string):
    r"""Takes a string of whitespace-separated tokens and parses it into a list.
    Whitespace inside tokens may be quoted with single quotes, double quotes or
    backslash (similar to command-line arguments in bash).

    >>> parse_args(r'''"a a" 'b b' c\ c "d'd" 'e"e' 'f\'f' "g\"g" "i""i" 'j''j' k" "k l' l' mm n\\n''')
    ['a a', 'b b', 'c c', "d'd", 'e"e', "f'f", 'g"g', 'ii', 'jj', 'k k', 'l l', 'mm', r'n\n']
    """
    re_split = re.compile(r'''((?:
        [^\s"'\\] |
        "(?: [^"\\] | \\.)*" |
        '(?: [^'\\] | \\.)*' |
        \\.
    )+)''', re.VERBOSE)
    args = re_split.split(string)[1::2]
    args = [re.compile(r'"((?:[^"\\]|\\.)*)"').sub('\\1', x) for x in args]
    args = [re.compile(r"'((?:[^'\\]|\\.)*)'").sub('\\1', x) for x in args]
    return [re.compile(r'\\(.)').sub('\\1', x) for x in args]

def exec_floe_class(klass, jvmtype="-server", jvmopts=[], extrajars=[], args=[], fork=False):
    global CONFFILE
    all_args = [
        "java", jvmtype,
        "-Dfloe.home=" + FLOE_HOME,
        "-Dfloe.config.file=" + CONFFILE,
        "-cp", get_classpath(extrajars),
    ] + jvmopts + CONFIG_OVERRIDES + [klass] + list(args)
    print "Running: " + " ".join(all_args)
    if fork:
        os.spawnvp(os.P_WAIT, "java", all_args)
    else:
        os.execvp("java", all_args) # replaces the current process and never returns

def jar(jarfile, klass, *args):
    """Syntax: [floe jar topology-jar-path class ...]

    Runs the main method of class with the specified arguments.
    The floe jars and configs in ~/.floe are put on the classpath.
    The process is configured so that floeSubmitter
    will upload the jar at topology-jar-path when the topology is submitted.
    """
    exec_floe_class(
        klass,
        jvmtype="-client",
        extrajars=[jarfile],
        args=args,
        jvmopts=[' '.join(filter(None, [JAR_JVM_OPTS, "-Dfloe.execution.jar=" +
        jarfile]))])

def kill(*args):
    """Syntax: [floe kill topology-name [-w wait-time-secs]]

    Kills the topology with the name topology-name. floe will
    first deactivate the topology's spouts for the duration of
    the topology's message timeout to allow all messages currently
    being processed to finish processing. floe will then shutdown
    the workers and clean up their state. You can override the length
    of time floe waits between deactivation and shutdown with the -w flag.
    """
    exec_floe_class(
        "backtype.floe.command.kill_topology",
        args=args,
        jvmtype="-client",
        extrajars=[USER_CONF_DIR, floe_DIR + "/bin"])


def coordinator(klass="edu.usc.pgroup.floe.coordinator.CoordinatorService"):
    """Syntax: [floe coordinator]

    Launches the floe coordinator. This command should be run under
    supervision with a tool like daemontools or monit.
    """
    jvmopts = [
        "-Dlogfile.name=coordinator.log",
        "-Dlogback.configurationFile=" + FLOE_HOME + "/conf/logback.xml",
    ]
    exec_floe_class(
        klass,
        jvmtype="-server",
        jvmopts=jvmopts,
        fork=True)

def container(klass="edu.usc.pgroup.floe.container.ContainerService"):
    """Syntax: [floe supervisor]

    Launches the floe container. This command should be run under
    supervision with a tool like daemontools or monit.
    """

    jvmopts = [
        "-Dlogfile.name=coordinator.log",
        "-Dlogback.configurationFile=" + FLOE_HOME + "/conf/logback.xml",
        ]
    exec_floe_class(
       klass,
       jvmtype="-server",
       jvmopts=jvmopts,
       fork=True)

def scale(*args):
    """
    Syntax: [floe scale -dir up/down -app <appname> -pellet <pelletname> -cnt <cnt>
    Sends the scale up/down command to the coordinator.
    """
    klass="edu.usc.pgroup.floe.client.commands.Scale"
    jvmopts = [
        "-Dlogfile.name=coordinator.log",
        "-Dlogback.configurationFile=" + FLOE_HOME + "/conf/logback.xml",
        ]
    exec_floe_class(klass,
                    jvmtype="-client",
                    jvmopts=jvmopts,
                    fork=False,
                    args=args)

def signal(*args):
    """
    Syntax: [floe signal -app <appname> -pellet <pelletname> -data <data>
    Sends the given signal command to the coordinator to be sent to the given app/pellet.
    """
    klass="edu.usc.pgroup.floe.client.commands.Signal"
    jvmopts = [
        "-Dlogfile.name=coordinator.log",
        "-Dlogback.configurationFile=" + FLOE_HOME + "/conf/logback.xml",
        ]
    exec_floe_class(klass,
                    jvmtype="-client",
                    jvmopts=jvmopts,
                    fork=False,
                    args=args)


def switch_alternate(*args):
    """
    Syntax: [floe signal -app <appname> -pellet <pelletname> -data <data>
    Sends the given signal command to the coordinator to be sent to the given app/pellet.
    """
    klass="edu.usc.pgroup.floe.client.commands.SwitchAlternate"
    jvmopts = [
        "-Dlogfile.name=coordinator.log",
        "-Dlogback.configurationFile=" + FLOE_HOME + "/conf/logback.xml",
        ]
    exec_floe_class(klass,
                    jvmtype="-client",
                    jvmopts=jvmopts,
                    fork=False,
                    args=args)

def dev_zookeeper():
    """Syntax: [floe dev-zookeeper]

    Launches a fresh Zookeeper server using "dev.zookeeper.path" as its local dir and
    "floe.zookeeper.port" as its port. This is only intended for development/testing, the
    Zookeeper instance launched is not configured to be used in production.
    """

    klass = "edu.usc.pgroup.floe.zookeeper.LocalZKServer"
    jvmopts = [
       "-Dlogfile.name=coordinator.log",
       "-Dlogback.configurationFile=" + FLOE_HOME + "/conf/logback.xml",
    ]
    exec_floe_class(
       klass,
       jvmtype="-server",
       jvmopts=jvmopts,
       fork=True)

def version():
  """Syntax: [floe version]

  Prints the version number of this floe release.
  """
  releasefile = floe_DIR + "/RELEASE"
  if os.path.exists(releasefile):
    print open(releasefile).readline().strip()
  else:
    print "Unknown"

def print_classpath():
    """Syntax: [floe classpath]

    Prints the classpath used by the floe client when running commands.
    """
    print get_classpath([])

def print_commands():
    """Print all client commands and link to documentation"""
    print "Commands:\n\t",  "\n\t".join(sorted(COMMANDS.keys()))
    print "\nHelp:", "\n\thelp", "\n\thelp <command>"
    print "\nDocumentation for the floe client can be found at https://github.com/nathanmarz/floe/wiki/Command-line-client\n"
    print "Configs can be overridden using one or more -c flags, e.g. \"floe list -c nimbus.host=nimbus.mycompany.com\"\n"

def print_usage(command=None):
    """Print one help message or list of available commands"""
    if command != None:
        if COMMANDS.has_key(command):
            print (COMMANDS[command].__doc__ or
                  "No documentation provided for <%s>" % command)
        else:
           print "<%s> is not a valid command" % command
    else:
        print_commands()

def unknown_command(*args):
    print "Unknown command: [floe %s]" % ' '.join(sys.argv[1:])
    print_usage()

COMMANDS = {"jar": jar, "kill": kill, "coordinator": coordinator,
            "container": container, "classpath": print_classpath,
            "dev-zookeeper": dev_zookeeper,
            "scale": scale,
            "signal": signal,
            "switch-alternate": switch_alternate,
            "help": print_usage}

def parse_config_opts(args):
    curr = args[:]
    curr.reverse()
    config_list = []
    args_list = []

    while len(curr) > 0:
        token = curr.pop()
        if token.startswith("-D"):
            config_list.append(token)
        else:
            args_list.append(token)

    return config_list, args_list

def main():
    if len(sys.argv) <= 1:
        print_usage()
        sys.exit(-1)
    #args = sys.argv[1:]
    global  CONFIG_OVERRIDES
    CONFIG_OVERRIDES, args = parse_config_opts(sys.argv[1:])
    COMMAND = args[0]
    ARGS = args[1:]
    (COMMANDS.get(COMMAND, unknown_command))(*ARGS)

if __name__ == "__main__":
    main()
