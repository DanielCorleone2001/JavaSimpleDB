package simpledb.storage;

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

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long fileLen = this.file.length();
        int numsPages = (int)Math.floor(fileLen * 1.0 / BufferPool.getPageSize());
        return numsPages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIterator iterator = new HeapFileIterator(tid, this);
        return iterator;
    }

    /**
     * use to iterator all the tuples of HeapFile
     * read page from BufferPool, or will occur Out of Memory !!!
     * because Physical Memory maybe can't load all tuples
     */
    public static class HeapFileIterator implements DbFileIterator {
        private final TransactionId transactionId;

        private final HeapFile heapFile;

        private Iterator<Tuple> iterator;

        private int pageNum;

        /**
         * Constructor of HeapFileIterator
         * @param transactionId
         * @param heapFile target HeapFile
         */
        public HeapFileIterator(TransactionId transactionId, HeapFile heapFile) {
            this.transactionId = transactionId;
            this.heapFile = heapFile;
        }

        /**
         * load tuple iterator from page which fetch from BufferPool
         * @param pageNum target PageNum, start from 0
         * @return Iterator of All tuples of the page which fetch from BufferPool
         * @throws DbException pageNum is invalid
         * @throws TransactionAbortedException
         */
        private Iterator<Tuple> getTupleIterator(int pageNum) throws DbException, TransactionAbortedException {
            if (pageNum < 0 || pageNum >= this.heapFile.numPages()) {
                throw new DbException("pageNum {" + pageNum + "} is not valid");
            }

            HeapPageId heapPageId = new HeapPageId(this.heapFile.getId(), pageNum);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.transactionId, heapPageId, Permissions.READ_ONLY);
            if (page == null) {
                throw  new DbException("can't find match page with pageNum {" + pageNum + "}");
            }
            return page.iterator();
        }

        /**
         *  Opens the iterator
         * @throws DbException  when there are problems opening/accessing the database.
         * @throws TransactionAbortedException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pageNum = 0;
            iterator = getTupleIterator(pageNum);
        }

        /**
         * check Has next or Not
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.iterator == null) return false; // iterator is not initial from getTupleIterator()

            if (this.pageNum >= heapFile.numPages()) return false; // pageNum is out of HeapFile len

            // at the end of iterator
            if (!this.iterator.hasNext() && pageNum == this.heapFile.numPages() - 1) return false;

            return true;
        }

        /**
         *  Gets the next tuple from the operator (typically implementing by reading
         *  from a child operator or an access method).
         * @return The next tuple in the iterator.
         * @throws DbException
         * @throws TransactionAbortedException
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(this.iterator == null) throw new NoSuchElementException("file not open, make sure open() before invoke this");

            if (!this.iterator.hasNext()) { // this page's iterator is null, fetch next page from BufferPool and read its tuple
                if (this.pageNum < this.heapFile.numPages() - 1) {
                    pageNum++;
                    this.iterator = getTupleIterator(pageNum);
                } else {
                    return null;
                }
            }
            return this.iterator.next();
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         * @throws TransactionAbortedException
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
            this.iterator = null;
        }
    }
}

