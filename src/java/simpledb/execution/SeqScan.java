package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;

    private int tableID;

    private String tableAlias;

    private DbFileIterator iterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tableAlias = tableAlias;
        this.tableID = tableid;
        this.transactionId = tid;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableID);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableID = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    /**
     * Opens the iterator. This must be called before any of the other methods.
     *
     * @throws DbException                 when there are problems opening/accessing the database.
     * @throws TransactionAbortedException
     */
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.iterator = Database.getCatalog().getDatabaseFile(tableID).iterator(transactionId);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableID);
        if (tableAlias == null) {
            return tupleDesc;
        }
        int numFields = tupleDesc.numFields();
        String[] fieldAr = new String[numFields]; //name
        Type[] typeAr = new Type[numFields]; // type
        for (int i = 0; i < numFields; i++) {
            fieldAr[i] = tableAlias + "." + tupleDesc.getFieldName(i);
            typeAr[i] = tupleDesc.getFieldType(i);
        }

        return new TupleDesc(typeAr,fieldAr);
    }

    /**
     * Returns true if the iterator has more tuples.
     *
     * @return true f the iterator has more tuples.
     * @throws IllegalStateException If the iterator has not been opened
     */
    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.iterator == null) return false;
        return iterator.hasNext();
    }

    /**
     * Returns the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return the next tuple in the iteration.
     * @throws NoSuchElementException if there are no more tuples.
     * @throws IllegalStateException  If the iterator has not been opened
     */
    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        if (iterator == null) {
            throw new NoSuchElementException("no next tuple");
        }
        Tuple t = iterator.next();
        if (t == null) {
            throw new NoSuchElementException("no next tuple");
        }

        return t;
    }

    /**
     * Closes the iterator. When the iterator is closed, calling next(),
     * hasNext(), or rewind() should fail by throwing IllegalStateException.
     */
    public void close() {
        // some code goes here
        iterator.close();
    }

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException           when rewind is unsupported.
     * @throws IllegalStateException If the iterator has not been opened
     */
    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }
}
