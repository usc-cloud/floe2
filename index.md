---
layout: page
title: Floe2
weight: 0
---

<table>
<TR>
	<TH> Feature </TH>
	<TH> Floe2  </TH>
	<TH> Storm </TH>
</TR>
<TR>
<TD> distributed stream computations </TD>
<TD> Y </TD>
<TD> Y </TD>
</TR>

<TR>
<TD> elastic deployment in response to changing data rates </TD>
<TD> Y </TD>
<TD> N </TD>
</TR>

<TR>
<TD> Stateful Operators/Bolts </TD>
<TD> Y </TD>
<TD> N </TD>
</TR>

<TR>
<TD> Fault Tolerance - Tuple Processing (atleast once gurantee) </TD>
<TD> Y, Peer backup, No Replay Required, Peer node takes over </TD>
<TD> Y, Upstream backup,  requires message replay  </TD>
</TR>

<TR>
<TD> Fault Tolerance - Operator State </TD>
<TD> Y, Peer Checkpointing </TD>
<TD> Not Applicable </TD>
</TR>

<TR>
<TD> Elastic "MapReduce" </TD>
<TD> Y (Number of mapper/reducer tasks can be increased/decreased at run-time based on the observed data rates without state or message loss) </TD>
<TD> Partially. (Number of reduce tasks cannot be changed, "rebalance" command may be used to leverage additional resources by moving tasks to newly available machines) </TD>
</TR>

</table>

Installation Guide
------------------
Available [here](INSTALL)
