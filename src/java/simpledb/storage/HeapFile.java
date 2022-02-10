package simpledb.storage;

import simpledb.storage.HeapPage;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

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

    private final File file;

    private final TupleDesc tupleDesc;

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
        return this.file;
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
        int uniqueID = this.file.getAbsoluteFile().hashCode();
        return uniqueID;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableID = pid.getTableId();
        int pageNumber = pid.getPageNumber();

        RandomAccessFile accessFile = null; // use randomAccess File to read and write pages at arbitrary offsets

        try {
            accessFile = new RandomAccessFile(this.file, "r");
            if ((pageNumber + 1) * BufferPool.getPageSize() > accessFile.length()) {
                accessFile.close();
                throw new IllegalArgumentException("No match Page in HeapFile");
            }

            byte[] bytes = new byte[BufferPool.getPageSize()];
            accessFile.seek(pageNumber * BufferPool.getPageSize()); // set the start pointer of file to read
            int len = accessFile.read(bytes, 0, BufferPool.getPageSize()); // read file' content into bytes array
            if (len != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableID, pageNumber, len));
            }

            HeapPageId pageId = new HeapPageId(tableID, pageNumber);
            HeapPage targetPage = new HeapPage(pageId, bytes);
            return targetPage;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                accessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableID, pageNumber));
    }


    /**
     * Push the specified page to disk.
     *
     * @param page The page to write.
     *             page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     */
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNumber = page.getId().getPageNumber();
        if (pageNumber > numPages()) {
            throw new IllegalArgumentException("page is not in the heap file or page'id in wrong");
        }

        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(pageNumber * BufferPool.getPageSize());
        byte[] pageData = page.getPageData();
        randomAccessFile.write(pageData);
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long fileLen = this.file.length();
        int numsPages = (int) Math.floor(fileLen * 1.0 / BufferPool.getPageSize());
        return numsPages;
    }

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to add.  This tuple should be updated to reflect that
     *            it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
//        ArrayList<Page> list = new ArrayList<>();
//        for (int i = 0; i < numPages(); i++) {
//            HeapPageId heapPageId = new HeapPageId(this.getId(), i);
//            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
//            if (heapPage.getNumEmptySlots() == 0) {
//                Database.getBufferPool().unsafeReleasePage(tid, heapPageId); // althrough this is not strict TPL, but it does not matter
//                continue;
//            }
//
//            heapPage.insertTuple(t);
//            list.add(heapPage);
//            return list;
//        }
//
//        // create a empty page and load with 0
//        BufferedOutputStream bufferOS = new BufferedOutputStream(new FileOutputStream(this.file, true));
//        byte[] emptyData = HeapPage.createEmptyPageData();
//        bufferOS.write(emptyData);
//        bufferOS.close();
//
//        // load into the BufferPool
//        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
//        page.insertTuple(t);
//        list.add(page);
//        return list;
        // not necessary for lab1
        ArrayList<Page> list = new ArrayList<>();
        BufferPool pool = Database.getBufferPool();
        int tableid = getId();
        for (int i = 0; i < numPages(); ++i) {
            HeapPage page = (HeapPage) pool.getPage(tid, new HeapPageId(tableid, i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                list.add(page);
                return list;
            }
        }

        HeapPage page = new HeapPage(new HeapPageId(tableid, numPages()), HeapPage.createEmptyPageData());
        page.insertTuple(t);
        writePage(page);
        return list;
    }

    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to delete.  This tuple should be updated to reflect that
     *            it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *                     of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        list.add(page);
        return list;
        // not necessary for lab1
    }

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIterator iterator = new HeapFileIterator(this, tid);
        return iterator;
    }


    private static final class HeapFileIterator implements DbFileIterator {

        private final HeapFile heapFile;

        private final TransactionId tid;

        private Iterator<Tuple> it;

        private int whichPage;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.heapFile = file;
            this.tid = tid;
        }

        /**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            whichPage = 0;
            it = getPageTuples(whichPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws DbException, TransactionAbortedException {
            if (pageNumber >= 0 && pageNumber < heapFile.numPages()) {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNumber);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            } else {
                throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber, heapFile.getId()));
            }
        }

        /**
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) {
                return false;
            }
            if (!it.hasNext()) {
                while (whichPage < (heapFile.numPages() - 1)) {
                    whichPage++;
                    it = getPageTuples(whichPage);
                    if (it.hasNext()) {
                        return it.hasNext();
                    }
                }
                if (whichPage >= (heapFile.numPages() - 1)) {
                    return false;
                }
            } else {
                return true;
            }
            return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null || !it.hasNext()) {
                throw new NoSuchElementException();
            }
            return it.next();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            it = null;
        }
    }


}





