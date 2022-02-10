## lab4

lab4需要实现事务，遵循严格的两阶段锁协议以及No-Steal, Force。**lab6后面会实现steal + no force**

什么是Steal和Force？

>steal/ no steal：是否将未commit的事务已经处理过的脏页从bufferpool中淘汰。
>
>如果淘汰的话就是steal，如果没有马上落盘，可能会数据丢失。
>
>force / no force：事务提交时是否强制将脏页落盘。
>
>如果强制落盘的话就是force，不强制就是no force。No force可能会导致数据的丢失。

### exercise1

对Page级别上锁，也就是BufferPool中的getPage需要上锁。

这里上的是**读写锁**。

为了方便管理，写了个LockManager，用两个ConcurrentHashMap来对读写锁进行控制

```java
private final Map<PageId, Map<TransactionId, Byte>> pageReadHolders;

private final Map<PageId, TransactionId> pageWriteHolder;
```

### exercise 2

考虑加锁解锁的时机，也就是锁的生命周期。

因为遵守两阶段锁协议，所以就是事务开始时上锁，commit之后再解锁。

唯一一个不遵从的地方是，但我们在page中找slot来插入tuple的时候，如果没有找到空的slot，那么这个page的锁其实是应该解掉的，尽管这不遵守两阶段锁协议，但是不影响事务。

### exercise 3

实现No Steal / Force。

这里的逻辑就是，在我们需要淘汰脏页时，不能将脏页淘汰，不然就不遵守no steal了。如果BufferPool满了，那就抛出异常。这里主要是简单的实现事务，让事务能跑起来把，后续在lab6会优化成steal + no force

### exercise 4

实现事务提交时的逻辑，也就是成功提交后将脏页全部落盘。这里就需要BufferPool中脏页的tid来判断是否是目标tid，是的话就将其落盘。

```java
 /**
  * Commit or abort a given transaction; release all locks associated to
  * the transaction.
  *
  * @param tid    the ID of the transaction requesting the unlock
  * @param commit a flag indicating whether we should commit or abort
  */
 public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            for (Page page : map.values()) {
                if (tid.equals(page.isDirty())) discardPage(page.getId());
            }
        }

        lockManager.releaseAll(tid);
    } 

/**
  * Write all pages of the specified transaction to disk.
  */
 public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Page page : map.values()) {
            if (tid.equals(page.isDirty())) {
                flushPage(page.getId());
                page.setBeforeImage();
            }
        }
    }
```

### exercise 5

避免死锁。

这里在分配读写锁的时候进行超时逻辑判断即可，更优方法是等待关系的依赖图。

```java
    /**
     *
     * @param tid
     * @param pid
     * @throws InterruptedException
     */
    private void acquireReadLock(TransactionId tid, PageId pid)
            throws InterruptedException {
        if (!hold(tid, pid)) {
            synchronized (pid) {
                Thread thread = Thread.currentThread();
                Timer timer = new Timer(true);
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        thread.interrupt();
                    }
                }, MINTIME + random.nextInt(MAXLEN));
                while (pageWriteHolder.containsKey(pid)) {
                    pid.wait(5);
                }
                timer.cancel();
                pageReadHolders.computeIfAbsent(pid, key ->
                        new ConcurrentHashMap<TransactionId, Byte>()).put(tid, FK);
            }
        }
    }

    /**
     *
     * @param tid
     * @param pid
     * @throws InterruptedException
     */
    private void acquireWriteLock(TransactionId tid, PageId pid)
            throws InterruptedException {
        if (!holdWriteLock(tid, pid)) {
            synchronized (pid) {
                Thread thread = Thread.currentThread();
                Timer timer = new Timer(true);
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        thread.interrupt();
                    }
                }, MINTIME + random.nextInt(MAXLEN));
                while (pageWriteHolder.containsKey(pid) ||
                        haveOtherReader(tid, pid)) {
                    pid.wait(5);
                }
                timer.cancel();
                pageWriteHolder.put(pid, tid);
            }
        }
    }
```

