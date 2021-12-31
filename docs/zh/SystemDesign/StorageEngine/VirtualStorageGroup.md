<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# 背景

存储组由用户显示指定，使用语句"SET STORAGE GROUP TO"来指定存储组，每一个存储组有一个对应的 StorageGroupProcessor

为了确保最终一致性，每一个存储组有一个数据插入锁（排它锁）来同步每一次插入操作。
所以服务端数据写入的并行度为存储组的数量。

# 问题

从背景中可知，IoTDB数据写入的并行度为 max(客户端数量，服务端数据写入的并行度)，也就是max(客户端数量，存储组数量)

在生产实践中，存储组的概念往往与特定真实世界实体相关（例如工厂，地点，国家等）。
因此存储组的数量可能会比较小，这会导致IoTDB写入并行度不足。即使我们开再多的客户端写入线程，也无法走出这种困境。

# Analyze

In IoTDB, every kinds of ingestion interface can only insert timeseries of on device.
One idea by intuition is changing the granularity of synchronization from storage group level to device level.
However, one lock for one device may occupy lots of resource that beyond our expectation. One lock means about 100 bytes of memory and one kernel object.
Sometimes the number of devices may reach one million and we can't afford such lock resource.

As the analysis progresses, we think we should trade off between granularity of synchronization and resource occupation.

# Solution

Our idea is to group devices into buckets and chang the granularity of synchronization from storage group level to device buckets level.

In detail, we use hash to group different devices into buckets called virtual storage group. 
For example, one device called "root.sg.d" is belonged to virtual storage group NO (hash("root.sg.d") mod num_of_virtual_storage_group)

# Usage

To use virtual storage group, you can set this config below:

```
virtual_storage_group_num
```

Recommended value is [virtual storage group number] = [CPU core number] / [user-defined storage group number]

For more information, you can refer to [this page](../../UserGuide/Appendix/Config-Manual.md).