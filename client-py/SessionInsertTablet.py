# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Uncomment the following line to use apache-iotdb module installed by pip3

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor, UseNew
from iotdb.utils.Tablet import Tablet
import random
import numpy as np
from numba import jit
# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
session.open(False)
import time

st = time.perf_counter()

@jit
def run():
    random.seed(a=0, version=2)
    insertCost = 0
    for i in range(2000, 4000):
        deviceId = "root.sg%d.%d" % (i % 8, i)
        measurements_ = ["s1",
                         # "is_true"
                         ]
        data_types_ = [
            TSDataType.FLOAT,
            # TSDataType.BOOLEAN,
        ]

        rowSize = 10080
        # rowSize = 5080
        if not UseNew:
            timestamps_ = []
            values_ = []
            # for t in range(0, 10080):
            for t in range(0, rowSize):
                timestamps_.append(t)
                # values_.append([-2 + 6 * random.random(), True])
                values_.append([-2 + 6 * 0.5
                                   # , True
                                ])
        else:
            timestamps_ = np.zeros(rowSize, dtype='>q')
            values_ = np.zeros(rowSize, dtype='>f')
            # for t in range(0, 10080):
            for t in range(0, rowSize):
                timestamps_[t] = t
                values_[t] = -2 + 6 * 0.5
                # values_.append([-2 + 6 * random.random(), True])

        tablet_ = Tablet(deviceId, measurements_, data_types_, values_, timestamps_)
        cost_st = time.perf_counter()
        session.insert_tablet(tablet_)
        insertCost += time.perf_counter() - cost_st

    session.close()
    end = time.perf_counter()

    print("All executions done!!")
    print("use time: %.3f" % (end - st))
    print("insert time: %.3f" % insertCost)

run()