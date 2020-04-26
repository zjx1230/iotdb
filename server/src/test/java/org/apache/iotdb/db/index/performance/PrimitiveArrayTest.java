package org.apache.iotdb.db.index.performance;//

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.rescon.PrimitiveArrayPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimitiveArrayTest {

  private static final Logger logger = LoggerFactory.getLogger(PrimitiveArrayTest.class);


  private final int threadCnt;
  ExecutorService pool;
  private int memoryThreshold;
  private final int totalWriteSize;
  private final AtomicInteger memoryUsed = new AtomicInteger();
  private boolean noMoreIndexFlushTask = false;
  private ConcurrentLinkedQueue flushTaskQueue = new ConcurrentLinkedQueue();
  private Future flushTaskFuture;
  public static final Object waitingSymbol = new Object();
  private String indexFilePath = "PrimitiveArrayTest.bin";
  private float sleepBase;

  public PrimitiveArrayTest(int threadCnt, int totalWriteNum, int memoryThreshold,
      float sleepBase) {
    this.memoryThreshold = memoryThreshold;
    this.sleepBase = sleepBase;
    this.threadCnt = threadCnt;
    this.pool = IoTDBThreadPoolFactory.newFixedThreadPool(threadCnt + 1, "PrimitiveArrayPool");
    this.totalWriteSize = totalWriteNum;
    memoryUsed.set(0);
  }

  public boolean syncAllocateSize(int mem) {
    int allowedMem = this.memoryThreshold - mem;
    int expectValue;
    while (true) {
      expectValue = memoryUsed.get();
      int targetValue = expectValue + mem;
      if (expectValue < allowedMem && memoryUsed.compareAndSet(expectValue, targetValue)) {
        // allocated successfully
        return true;
      }
      synchronized (waitingSymbol) {
        try {
          waitingSymbol.wait();
        } catch (InterruptedException e) {
          logger.error("interrupted, canceled");
          return false;
        }
      }
    }
  }

  public Runnable flushRunTask = () -> {
    boolean returnWhenNoTask = false;
    while (true) {
      if (noMoreIndexFlushTask) {
        returnWhenNoTask = true;
      }
      Object indexFlushMessage = flushTaskQueue.poll();
      if (indexFlushMessage == null) {
        if (returnWhenNoTask) {
          break;
        }
        try {
          Thread.sleep(10);
        } catch (@SuppressWarnings("squid:S2142") InterruptedException e) {
          logger.error("Index Flush Task is interrupted, index path {}", indexFilePath, e);
          break;
        }
      } else {
        try {
          //Simulate IO consume
          Thread.sleep(20);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
//        WriteStruct writeStruct = (WriteStruct) indexFlushMessage;
//        int after = writeStruct.release(memoryUsed);
        int after = 0;
        if (after < memoryThreshold) {
          synchronized (waitingSymbol) {
            waitingSymbol.notifyAll();
          }
          System.out.println(String.format("Flush release, now %d < thres, notify all", after));
        } else {
          System.out.println(String.format("Flush release, now %d > thres, keep release", after));
        }
      }
    }
  };

  public void go() {
    // start
    this.flushTaskQueue = new ConcurrentLinkedQueue();
    this.flushTaskFuture = pool.submit(flushRunTask);
    this.noMoreIndexFlushTask = false;
    // run
    for (int i = 0; i < threadCnt; i++) {
      pool.submit(new NativeListInstance(totalWriteSize, sleepBase));
    }
    //end, never end, keep going
  }

  public static class PrimitiveLongList {

    List<long[]> values = new ArrayList<>();
    private PrimitiveArrayTest test;

    public PrimitiveLongList(PrimitiveArrayTest test) {
      this.test = test;
      values.add((long[]) PrimitiveArrayPool.getInstance().
          getPrimitiveDataListByType(TSDataType.FLOAT));
    }

    public void putSome(int size) {
      expand();
    }

    private void expand() {
      test.syncAllocateSize(0);
      values.add((long[]) PrimitiveArrayPool
          .getInstance().getPrimitiveDataListByType(TSDataType.INT64));
    }
  }

  
  public static class NativeListInstance implements Runnable {

    public int totalWriteSize;
    public float sleepBase;

    public NativeListInstance(int totalWriteSize, float sleepBase) {
      this.totalWriteSize = totalWriteSize;
      this.sleepBase = sleepBase;

    }



    @Override
    public void run() {
      while (true) {

      }
    }
  }

  public static void main(String[] args) {

//    a.putFloat(1, 1);
  }

}
