package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {                                              // page id = pid's page
        // some code goes here
        try {
            RandomAccessFile file = new RandomAccessFile(this.file, "r");
            int pageSize = BufferPool.getPageSize();                                // Bytes per page
            byte[] buffer = new byte[pageSize];
            try {
                file.seek((long) pid.getPageNumber() * pageSize);
                if (-1 == file.read(buffer))
                    return null;
            } catch (IOException e) {
                return null;
            }
            file.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), buffer);
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            PageId pid = page.getId();
            int pageSize = BufferPool.getPageSize();
            randomAccessFile.seek((long) pid.getPageNumber() * pageSize);
            randomAccessFile.write(page.getPageData());
            randomAccessFile.close();
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return ((int) Math.ceil(file.length() * 1.0 / BufferPool.getPageSize()));
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> resultList = new ArrayList<>();
        // if has empty page and empty slot to insert
        for (int i = 0; i < numPages(); ++i) {
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);   // by heapPagId to get this page
            if (heapPage.getNumEmptySlots() > 0) {
                heapPage.insertTuple(t);
                resultList.add(heapPage);
                return resultList;
            }
        }
        // no empty page, we need to realloc page and write to disk
        HeapPage heapPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        if (heapPage.getNumEmptySlots() > 0) {
            heapPage.insertTuple(t);
            writePage(heapPage);
            resultList.add(heapPage);
        }
        return resultList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> resultList = new ArrayList<>();
        if (t.getRecordId().getPageId().getTableId() != (getId()))
            throw new DbException("the tuple cannot be deleted or is not a member of the file");
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        resultList.add(heapPage);
        return resultList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

    public static final class HeapFileIterator implements DbFileIterator {
        // some code goes here
        private final HeapFile heapFile;
        private final TransactionId tid;
        private boolean isOpen = false;             // The db file is open or not
        private Integer pid = 0;                    // the heap page id indicates current heap page
        private HeapPage curPage;                   // current heap page
        private Iterator<Tuple> curTupleIter;       // current page's tuple's iter

        public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
            this.tid = tid;
            this.heapFile = heapFile;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.isOpen = true;
            this.curTupleIter = readPage(pid);
        }

        public Iterator<Tuple> readPage(int pid) throws TransactionAbortedException, DbException {
            if (pid >= 0 && pid < heapFile.numPages()) {
                curPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(heapFile.getId(), pid), Permissions.READ_ONLY);
                return curPage.iterator();
            } else {
                throw new DbException("read page error");
            }
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException, DbException {
            if (!isOpen || curTupleIter == null)
                return false;
            while (curTupleIter != null && !curTupleIter.hasNext()) {
                if (pid < (heapFile.numPages() - 1)) {
                    pid++;
                    curTupleIter = readPage(pid);
                } else
                    curTupleIter = null;
            }
            return curTupleIter != null;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (curTupleIter == null || !curTupleIter.hasNext())
                throw new NoSuchElementException();
            return curTupleIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
            pid = 0;
        }

        @Override
        public void close() {
            isOpen = false;
            pid = 0;
            curTupleIter = null;
            curPage = null;
        }
    }
}

