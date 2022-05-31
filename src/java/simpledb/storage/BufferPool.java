package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.storage.evict.EvictStrategy;
import simpledb.storage.evict.LRU;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.transaction.lock.LockManager;
import simpledb.transaction.lock.PageLock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    private final ConcurrentHashMap<PageId, Page> pages;              // pages stored in BufferPool
    private final ReadWriteLock lock;                                 // control read/write access privilege
    private final EvictStrategy evictStrategy;                        // evict strategy
    private final LockManager lockManager;                            // lock controller

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pages = new ConcurrentHashMap<>(numPages);
        lock = new ReentrantReadWriteLock();
        evictStrategy = new LRU();
        lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }


    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        PageLock.LockType lockType = perm == Permissions.READ_WRITE ? PageLock.LockType.EXCLUSIVE : PageLock.LockType.SAHRE;
        long startTime = System.currentTimeMillis();
        long timeout = 1000;
        boolean isAcquired = false;
        try {
            while (!isAcquired) {
                long endTime = System.currentTimeMillis();
                if (endTime - startTime <= timeout) {
                    isAcquired = lockManager.acquireLock(tid, pid, lockType);
                } else
                    throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Page targetPage = pages.get(pid);
        if (targetPage == null) {
            if (pages.size() >= DEFAULT_PAGES)
                evictPage();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            targetPage = dbFile.readPage(pid);
            pages.put(pid, targetPage);
        }
        return targetPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

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
        if (commit) {                       // flush dirty pages associated to the transaction to disk
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else
            restoreState(tid);            // revert any changes made by the transaction by restoring the page to its on-disk state
        lockManager.releaseAll(tid);      // release locks the trx holds
    }

    private void restoreState(TransactionId tid) {
        for (PageId pid : pages.keySet()) {
            Page page = pages.get(pid);
            if (page.isDirty() == tid) {
                int tableId = pid.getTableId();
                DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
                Page originalPage = dbFile.readPage(pid);
                pages.put(pid, originalPage);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> arrayList = (ArrayList<Page>) file.insertTuple(tid, t);
        for (Page p : arrayList) {
            p.markDirty(true, tid);
            if (pages.size() > DEFAULT_PAGES)
                evictPage();
            pages.put(p.getId(), p);        // Important!
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        if (pid == null)
            return;
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        ArrayList<Page> arrayList = (ArrayList<Page>) file.deleteTuple(tid, t);
        for (Page p : arrayList) {
            p.markDirty(true, tid);
            if (pages.size() > DEFAULT_PAGES)
                evictPage();
            pages.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
            PageId pageId = entry.getKey();
            Page page = entry.getValue();
            if (page.isDirty() != null) {
                flushPage(pageId);
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    public synchronized void flushPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        Page flushedPage = pages.get(pid);
        // check the page is whether dirty
        try {
            if (flushedPage.isDirty() != null) {
                Database.getLogFile().logWrite(flushedPage.isDirty(), flushedPage.getBeforeImage(), flushedPage);
                Database.getLogFile().force();
            }
            int tableId = pid.getTableId();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);     // find the dbfile
            dbFile.writePage(flushedPage);                                      // write the page to dbfile
            flushedPage.markDirty(false, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
            Page page = entry.getValue();
            if (page.isDirty() == tid) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * NO STEAL Strategy
     */
    private synchronized void evictPage() throws DbException {
        PageId evictPageId = evictStrategy.getEvictPageId(pages);
        if (evictPageId == null) throw new DbException("There are no satisfying pages to evict in the bp");
        this.flushPage(evictPageId);
        discardPage(evictPageId);
    }
}

