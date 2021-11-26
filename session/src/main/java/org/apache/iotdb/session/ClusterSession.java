/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.session;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class ClusterSession {
  Session[] sessions;
  ArrayBlockingQueue[] queues;
  List<EndPoint> nodeList;

  public ClusterSession(String host, int rpcPort) throws IoTDBConnectionException {
    Session session = new Session(host, rpcPort);
    session.open();
    nodeList = new ArrayList<>();
    nodeList.addAll(session.getNodeList());
    sessions = new Session[nodeList.size()];
    queues = new ArrayBlockingQueue[nodeList.size()];
    for (int i = 0; i < nodeList.size(); i++) {
      sessions[i] = new Session(nodeList.get(i).ip, nodeList.get(i).port);
      sessions[i].open();
      queues[i] = new ArrayBlockingQueue<>(1000);
      new Thread(new RunnableTask(i)).start();
    }
  }

  public void insertTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException {
    int hashVal = Math.abs(tablet.prefixPath.hashCode());
    int index = hashVal % nodeList.size();
    for (int i = 0; i < 2; i++) {
      int j = (index + i) % nodeList.size();
      synchronized (queues[j]) {
        if (!queues[j].isEmpty()) {
          queues[j].add(tablet);
          queues[j].notifyAll();
          continue;
        }
      }
      try {
        sessions[j].insertTablet(tablet);
      } catch (Exception e) {
        synchronized (queues[j]) {
          queues[j].add(tablet);
          queues[j].notifyAll();
        }
      }
    }
  }

  public SessionDataSet queryTablet(String sql, String deviceId) {
    int hashVal = Math.abs(deviceId.hashCode());
    int index = hashVal % nodeList.size();
    SessionDataSet sessionDataSet = null;
    try {
      sessionDataSet = sessions[index].executeQueryStatement(sql);
    } catch (Exception e) {
      try {
        sessionDataSet = sessions[(index + 1) % nodeList.size()].executeQueryStatement(sql);
      } catch (Exception ex) {
        // never happen, once the node restart, it won't be killed anymore.
        e.printStackTrace();
      }
    }
    return sessionDataSet;
  }

  public Session newSession(int index) throws IoTDBConnectionException {
    Session session = new Session(nodeList.get(index).ip, nodeList.get(index).port);
    session.open();
    return session;
  }

  class RunnableTask implements Runnable {
    int index;

    public RunnableTask(int index) {
      this.index = index;
    }

    @Override
    public void run() {
      while (true) {
        synchronized (queues[index]) {
          while (queues[index].isEmpty()) {
            try {
              queues[index].wait(1000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }

        Tablet t = null;
        try {
          Session session = newSession(index);
          while (!queues[index].isEmpty()) {
            t = (Tablet) queues[index].poll();
            session.insertTablet(t);
          }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          if (t != null) {
            queues[index].add(t);
          }
          try {
            Thread.sleep(3000);
          } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
          }
        }
      }
    }
  }
}
