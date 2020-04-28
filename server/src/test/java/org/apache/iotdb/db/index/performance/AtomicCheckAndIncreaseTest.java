package org.apache.iotdb.db.index.performance;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.index.IndexFileProcessor;
import org.junit.Test;

/**
 * check the correctness of flushRunTask in {@linkplain IndexFileProcessor} in parallel
 *
 */
public class AtomicCheckAndIncreaseTest {

  final Object waitingSymbol = new Object();
  int memoryThreshold;
  AtomicInteger memoryUsed = new AtomicInteger(0);
  private Random r = new Random();
  private int totalConsum = 0;

  public AtomicCheckAndIncreaseTest(int memoryThreshold) {
    this.memoryThreshold = memoryThreshold;
  }

  public boolean noMoreIndexFlushTask = false;

  public Runnable flushRunTask = () -> {
    while (true) {
      int curMem = memoryUsed.get();
      if (curMem > 0) {
        int delta = curMem < 10 ? curMem : r.nextInt(curMem);
        int after = memoryUsed.addAndGet(-delta);
        totalConsum += delta;
        if (after < memoryThreshold) {
          synchronized (waitingSymbol) {
            waitingSymbol.notifyAll();
          }
          System.out.println(String
              .format(
                  "Flush Thread consume %d, total consume %d, from %d to %d < threshold, wait all and keep minus",
                  delta, totalConsum, curMem, after));
        } else {
          System.out.println(String
              .format(
                  "Flush Thread consume %d, total consume %d, from %d to %d > threshold, keep minus",
                  delta, totalConsum, curMem, after));
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        System.out.println("Index Flush Task is interrupted, index path");
        break;
      }
      System.out.println("flush thread, wait 1s");
    }
  };

  public boolean syncAllocateSize(int mem) {
    int allowedMem = memoryThreshold - mem;
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
          System.out.println("interrupted, canceled");
          return false;
        }
      }
    }
  }

  public static class Increase implements Runnable {

    private final int id;
    private final long startTime;
    private int target;
    private AtomicCheckAndIncreaseTest aaa;
    private int current;
    private Random r;

    public Increase(int id, int target, AtomicCheckAndIncreaseTest aaa) {
      this.id = id;
      this.target = target;
      this.aaa = aaa;
      this.current = 0;
      r = new Random();
      startTime = System.currentTimeMillis();
    }

    public void run() {
      while (current < target) {
        int toAdd = target - current >= 10 ? r.nextInt(10) : target - current;
        if (aaa.syncAllocateSize(toAdd)) {
          System.out.println(String
              .format("Thread %d increase %d, from %d to %d", id, toAdd, current, current + toAdd));
          current += toAdd;
        } else {
          System.out.println(String.format("Thread %d error break, current: %d", id, current));
        }
      }
      System.out.println(String.format("Thread %d finish, current %d, use %d ms", id, current,
          System.currentTimeMillis() - startTime));
    }

  }

  @Test
  public void testFlushRunTask() {
    AtomicCheckAndIncreaseTest test = new AtomicCheckAndIncreaseTest(100);
    ExecutorService executor = Executors.newCachedThreadPool();
    executor.submit(test.flushRunTask);
    for (int i = 0; i < 20; i++) {
      Increase inc = new Increase(i, 50, test);
      executor.submit(inc);
    }
  }
}
