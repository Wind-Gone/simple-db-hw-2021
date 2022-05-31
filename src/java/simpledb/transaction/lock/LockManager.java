package simpledb.transaction.lock;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Hu Zirui
 * @version 1.0.0
 * @ClassName LockManager.java
 * @Description TODO
 * @createTime 2022年05月31日 11:20:00
 */
public class LockManager {
    private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> pageLocks;

    public LockManager() {
        pageLocks = new ConcurrentHashMap<>();
    }

    public LockManager(ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> pageLocks) {
        this.pageLocks = pageLocks;
    }

    /**
     * LockManager is responsible for acquire locks, release locks and view whether dedicated page has locks
     *
     * @param pageId
     * @param transactionId
     * @param lockType
     * @return boolean
     * @throws TransactionAbortedException
     * @throws InterruptedException
     */
    public synchronized boolean acquireLock(TransactionId transactionId, PageId pageId, PageLock.LockType lockType) throws InterruptedException, TransactionAbortedException {
        ConcurrentHashMap<TransactionId, PageLock> lockMap = pageLocks.get(pageId);
        // if this page has no locks, we can directly put it into map
        if (lockMap == null) {
            PageLock pageLock = new PageLock(lockType);
            lockMap = new ConcurrentHashMap<>();
            lockMap.put(transactionId, pageLock);
            pageLocks.put(pageId, lockMap);
            return true;
        }
        // has locks (we need to check its type)
        PageLock pageLock = lockMap.get(transactionId);
        if (pageLock == null) {
            if (lockType == PageLock.LockType.EXCLUSIVE) {
                Thread.sleep(5);                        // sleep for a while and then return to avoid repeated compete
                return false;
            } else {
                if (lockMap.size() > 1) {                   // that means there have multiple read locks in this page accessed by different trx
                    PageLock pageLock1 = new PageLock(lockType);
                    lockMap.put(transactionId, pageLock1);
                    pageLocks.put(pageId, lockMap);
                    return true;
                } else {                                    // only one lock , we need to check its type
                    PageLock pageLock1 = null;
                    Map.Entry<TransactionId, PageLock> entry = lockMap.entrySet().iterator().next();
                    pageLock1 = entry.getValue();
                    if (pageLock1.lockType == PageLock.LockType.EXCLUSIVE) {
                        Thread.sleep(5);
                        return false;
                    }
                    if (pageLock1.lockType == PageLock.LockType.SAHRE) {
                        lockMap.put(transactionId, pageLock1);
                        pageLocks.put(pageId, lockMap);
                        return true;
                    }
                }
            }
        } else {
            if (lockType == PageLock.LockType.SAHRE) {                                              // r-? return true directly
                return true;
            } else {
                if (lockMap.size() > 1)                                                             // deadlock
                    throw new TransactionAbortedException();
                if (pageLock.lockType == PageLock.LockType.EXCLUSIVE)                               // w-w lock return true
                    return true;
                if (pageLock.lockType == PageLock.LockType.SAHRE) {                                 // w-r lock, lock upgrade
                    PageLock pageLock1 = new PageLock(PageLock.LockType.EXCLUSIVE);
                    lockMap.put(transactionId, pageLock1);
                    pageLocks.put(pageId, lockMap);
                }
            }
        }
        return true;
    }

    public synchronized void releaseLock(TransactionId transactionId, PageId pageId) {
        ConcurrentHashMap<TransactionId, PageLock> lockMap = pageLocks.get(pageId);
        if (lockMap != null) {
            PageLock lock = lockMap.get(transactionId);
            if (lock != null)
                lockMap.remove(transactionId);
            if (lockMap.size() == 0)
                pageLocks.remove(pageId);
        }
    }

    public synchronized boolean holdsLock(TransactionId transactionId, PageId pageId) {
        return pageLocks.containsKey(pageId) && pageLocks.get(pageId) != null && pageLocks.get(pageId).containsKey(transactionId);
    }

    public void releaseAll(TransactionId transactionId) {
        ConcurrentHashMap.KeySetView<PageId, ConcurrentHashMap<TransactionId, PageLock>> pageIds = pageLocks.keySet();
        for (PageId pageId : pageIds) {
            releaseLock(transactionId, pageId);
        }
    }

}
