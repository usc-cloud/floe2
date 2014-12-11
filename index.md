---
layout: page
title: Floe2
weight: 0
---

Floe2 is a distributed, elastic and adaptive framework for large-scale dynamic streaming applications. Floe2 enables reliable processing of continuous high velocity data streams from varied data sources. 

A distinguishing feature of Floe2 compared to other stream processing systems such as [Storm](http://storm.apache.org) and [Infosphere](http://www-01.ibm.com/software/data/infosphere/) is it's focus on run-time adaptability to support long running mission-critical applications. This allows Floe2 applications to go beyond traditional data mining and analysis and support complex real-world applications comprising data collection, analytics, control, and finally feedback and adaptations that allows the application to evolve over time.


The table below presents a feature comparison between Floe2 and Storm, a widely used stream processing system.

<table width="100%">
<TR>
	<TH> Feature </TH>
	<TH width="5%"> Floe2  </TH>
	<TH width="5%"> Storm </TH>
	<TH> Notes </TH>
</TR>
<TR>
<TD> distributed stream computations </TD>
<TD> Y </TD>
<TD> Y </TD>
<TD> Both Floe2 and Storm support similar stream processing application topology composed of Processing Elements (Spout/Bolt eqiv. in storm) connected using data flow edges to direct the flow of data streams among them. Both support a variety of Data Stream sources such as RabbitMQ/AMQP, Kafka, JMS, Kestrel and can easily integrate with any existing data base systems. Further, applications built using Storm are easily portable to Floe2. </TD>
</TR>

<TR>
<TD> auto-scaling/elastic deployment in response to changing stream data rates </TD>
<TD> Y </TD>
<TD> N </TD>
<TD> Floe2 supports and a *period scheduler* that can provision additional resources to support variable data rates WITHOUT the need to pause the application. </TD>
</TR>

<TR>
<TD> Stateful Operators/Bolts </TD>
<TD> Y </TD>
<TD> N </TD>
<TD> NOTE </TD>
</TR>

<TR>
<TD> Fault Tolerance - Tuple Processing (atleast once gurantee) </TD>
<TD> Y </TD>
<TD> Y </TD>
<TD> Peer backup, No Replay Required, Peer node takes over. Upstream backup,  requires message replay </TD>
</TR>

<TR>
<TD> Fault Tolerance - Operator State </TD>
<TD> Y </TD>
<TD> N/A </TD>
<TD> Note </TD>
</TR>

<TR>
<TD> Elastic "MapReduce" </TD>
<TD> Y </TD>
<TD> N </TD>
<TD> (Number of mapper/reducer tasks can be increased/decreased at run-time based on the observed data rates without state or message loss). (Number of reduce tasks cannot be changed, "rebalance" command may be used to leverage additional resources by moving tasks to newly available machines) </TD>
</TR>

</table>