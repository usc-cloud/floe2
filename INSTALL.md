#Installation Guide
---------------------
The current verion of Floe2 has been tested for Ubuntu 14.04 hosts and the following instructions are based on that. Support for other platforms is coming soon.

Follow the below steps for each machine hosting any of the Floe2 components (e.g. coordinator, container). For client machine, you may skip zmq and jzmq installation.

##Pre-requisites
----------------
1. zmq 4.0.4+ (container nodes)
2. jzmq (container nodes)
3. python 2.7.x

###Installing zmq

Download and install zmq 4.0.4+ from [ZMQ Download Page](http://zeromq.org/intro:get-the-software). Instructions are repeated here for convenience, however it is suggested to follow the latest instructions on the ZMQ site.

```bsh
    #pre-reqs for installing zmq.
    sudo apt-get install g++ gcc libtool autoconf automake make
        
    #download and extract zmq.    
    wget http://download.zeromq.org/zeromq-4.0.4.tar.gz
    tar xvzf zeromq-4.0.4.tar.gz
    cd zeromq-4.0.4
    
    #configure and install using default parameters.
    ./configure
    ./make 
    ./make install
```        
        
###Installing jzmq - Java bindings for ZMQ
Download and install jzmq from [JZMQ Download Page](http://zeromq.org/bindings:java). Instructions are repeated here for convenience, however it is suggested to follow the latest instructions on the ZMQ site.

```bsh
    #pre-reqs for installing jzmq.
    sudo apt-get install autoconf automake libtool gcc g++ make libuuid-dev git openjdk-7-jdk pkg-config
   


    #download the v3.1.0 tar from [jzmq github archive](https://github.com/zeromq/jzmq/archive/v3.1.0.tar.gz).

    wget https://github.com/zeromq/jzmq/archive/v3.1.0.tar.gz
    tar xvzf v3.1.0.tar.gz
    cd jzmq-3.1.0
    

    #configure, build and install jzmq.
    ./autogen.sh
    ./configure
    make
    sudo make install
    
    #configure environment.
    sudo bash
        
    #For non-debian systems.
    echo /usr/local/lib > /etc/ld.so.conf.d/local.conf
    ldconfig
    
    #configure LD_LIBRARY_PATH
    echo export LD_LIBRARY_PATH=/usr/local/lib > /etc/profile.d/ldlibrarypath.sh  
    exit
    
    
    #LOG OUT AND LOG BACK IN (or restart the system)
    
    #verify zmq and jzmq
    echo $LD_LIBRARY_PATH
    #you shoud see "/usr/local/lib" as output.
    
    cd <jzmq dir>/src/main/perf
    java local_lat tcp://127.0.0.1:5000 1 100
    
    #This should run without errors. You won't see any output on console if it runs successfully. If you see errors, please visit [zmq java binding page](http://zeromq.org/bindings:java) for troubleshooting information.
```        


##Download, build and configure Floe2 
-------------------------------------

###Configure Floe2
Floe2 can be run in several modes:

1. ***Local Mode:*** - Single Process (used for development and debugging)  
 All components of Floe2 run in a single process (including zookeeper, coordinator, container). User just executes the user application (see examples/HelloWorld) and the framework launches all the components in a single process.  
 
2. ***Psuedo Distributed Mode:*** - Single Machine, multi procs. (used for testing the application in an environment similar to the distributed version before actual deployment on a cluster)  
 All components of Floe2 run on a single machine but in seperate processes. User is responsible for running each component manually. However, currently, you can run only one instance of container (will be fixed in next version).
 
3. ***Distributed Mode:*** - Cluster of Machines (distributed deployment of Floe2)
Components span multiple machines with (preferably) separate machines dedicated for different components. One dedicated machine for Zookeeper, one for Coordinator and Resource Manager, and multiple machines for containers (one container per machine).

---
####_Local Mode_
---

**Download and Build Floe2**   
(A release version is not available yet, use the github master branch)

```
    #install maven.
    sudo apt-get install maven python
    
    #clone github master branch.
    git clone https://github.com/usc-cloud/floe2.git
    
    #compile and install using maven.
    cd floe2
    mvn install  
```

**Configure Floe2 and run in local mode**

* Change ***floe.execution.mode*** in file conf/floe.properties to **local**  
 Known Issue: After changing a config value, you have to run ``mvn install'' again. This will be fixed in the coming version.


**Compile and Run Sample**  
(Use floe-examples project as a template for creating your project)   
floe-examples is compiled along with floe-core during the previous step.
```
    #to run the sample HelloWorld Application. (go to the floe2 home directory)
    chmod a+x bin/floe.py
    bin/floe.py jar floe-examples/target/floe-examples-0.1-SNAPSHOT.jar edu.usc.pgroup.HelloWorldApp
```
---
####_Psuedo distributed Mode_
---
**Download and Build Floe2**   
Follow same instructions as ***Local Mode***

**Configure Floe2 and run in psuedo distributed mode**

* Change ***floe.execution.mode*** in file conf/floe.properties to **distributed**  
* Leave other properties such **floe.zk.servers**, **floe.coordinator.host** etc. set to **localhost**  
 Known Issue: After changing a config value, you have to run ``mvn install'' again. This will be fixed in the coming version.

**Compile and Run Sample**  
(Use floe-examples project as a template for creating your project)   
floe-examples is compiled along with floe-core during the previous step.
```
    chmod a+x bin/floe.py
    
    #Start zookeeper. (For convinience, a dev version of zookeeper is provided with Floe2. Do not use this for production environment.)
    bin/floe.py dev-zookeeper &
    
    #Start coordinator
    bin/floe.py coordinator &    
    
    #Start container
    bin/floe.py container &    
    
    #to run the sample HelloWorld Application. (go to the floe2 home directory)
    bin/floe.py jar floe-examples/target/floe-examples-0.1-SNAPSHOT.jar edu.usc.pgroup.HelloWorldApp
```


---
####_Distributed Mode_
---
Coming soon.