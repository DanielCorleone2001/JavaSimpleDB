package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private static final Byte FK = 0;
    private static final int MINTIME = 100, MAXLEN = 900;
    private final Random random = new Random();

    private final Map<PageId, Map<TransactionId, Byte>> pageReadHolders;
    private final Map<PageId, TransactionId> pageWriteHolder;


    public LockManager() {
        this.pageReadHolders = new ConcurrentHashMap<PageId, Map<TransactionId,Byte>>();
        this.pageWriteHolder = new ConcurrentHashMap<PageId, TransactionId>();

    }

    /**
     *
     * @param tid
     * @param pid
     * @param p
     * @throws TransactionAbortedException
     */
    public void acquire(TransactionId tid, PageId pid, Permissions p)
            throws TransactionAbortedException {

        try {
            if (p == Permissions.READ_ONLY) {
                acquireReadLock(tid, pid);
            } else {
                acquireWriteLock(tid, pid);
            }
        } catch (InterruptedException e) {
            throw new TransactionAbortedException();
        }

    }

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

    /**
     *
     * @param tid
     * @param pid
     * @return
     */
    private boolean haveOtherReader(TransactionId tid, PageId pid) {
        synchronized (pid) {
            if (pageReadHolders.containsKey(pid)) {
                for (TransactionId t : pageReadHolders.get(pid).keySet()) {
                    if (!t.equals(tid)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     *
     * @param tid
     * @param pid
     */
    public void release(TransactionId tid, PageId pid) {
        releaseWriteLock(tid, pid);
        releaseReadLock(tid, pid);
    }

    /**
     *
     * @param tid
     * @param pid
     */
    private void releaseReadLock(TransactionId tid, PageId pid) {
        if (holdReadLock(tid, pid)) {
            synchronized (pid) {
                pageReadHolders.get(pid).remove(tid);
                if (pageReadHolders.get(pid).isEmpty()) {
                    pageReadHolders.remove(pid);
                }
            }
        }
    }

    /**
     *
     * @param tid
     * @param pid
     */
    private void releaseWriteLock(TransactionId tid, PageId pid) {
        if (holdWriteLock(tid, pid)) {
            synchronized (pid) {
                pageWriteHolder.remove(pid);
            }
        }
    }

    /**
     *
     * @param tid
     * @param pid
     * @return
     */
    public boolean hold(TransactionId tid, PageId pid) {
        return holdReadLock(tid, pid) || holdWriteLock(tid, pid);
    }

    /**
     *
     * @param tid
     * @param pid
     * @return
     */
    private boolean holdReadLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            return pageReadHolders.containsKey(pid) &&
                    pageReadHolders.get(pid).containsKey(tid);
        }
    }

    /**
     *
     * @param tid
     * @param pid
     * @return
     */
    private boolean holdWriteLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            return pageWriteHolder.containsKey(pid) &&
                    pageWriteHolder.get(pid).equals(tid);
        }
    }

    /**
     *
     * @param tid
     */
    public void releaseAll(TransactionId tid) {
        for (PageId pid : pageWriteHolder.keySet()) {
            releaseWriteLock(tid, pid);
        }
        for (PageId pid : pageReadHolders.keySet()) {
            releaseReadLock(tid, pid);
        }
    }
}
