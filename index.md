---
layout: page
title: Floe2
weight: 0
---


| Feature | Floe2 | Storm |
|------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| distributed stream computations | Y | Y |
| elastic deployment in response to changing data rates | Y | N |
| Stateful Operators/Bolts | Y | N |
| Fault Tolerance - Tuple Processing (atleast once gurantee) | Y, Peer backup, No Replay Required, Peer node takes over | Y, Upstream backup,  requires message replay  |
| Fault Tolerance - Operator State | Y, Peer Checkpointing | Not Applicable |
| Elastic "MapReduce" | Y (Number of mapper/reducer tasks can be increased/decreased at run-time based on the observed data rates without state or message loss) | Partially. (Number of reduce tasks cannot be changed, "rebalance" command may be used to leverage additional resources by moving tasks to newly available machines) |


Installation Guide
------------------
Available [here](INSTALL)
