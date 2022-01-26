package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;

    private OpIterator child;

    private int tableID;

    private final TupleDesc td;

    private boolean state;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableID = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"number of inserted tuples"});
        this.state = false;
    }

    /**
     * @return return the TupleDesc of the output tuples of this operator
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    /**
     * Closes this iterator. If overridden by a subclass, they should call
     * super.close() in order for Operator's internal state to be consistent.
     */
    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException           when rewind is unsupported.
     * @throws IllegalStateException If the iterator has not been opened
     */
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        // return null if called more than once
        if (state) return null;

        Tuple t = new Tuple(this.td);
        int cnt = 0;
        BufferPool bufferPool = Database.getBufferPool();
        while (child.hasNext()) {
            try {
                bufferPool.insertTuple(tid, this.tableID, child.next());
            } catch (IOException e) {
                throw new DbException("fail to insert tuple");
            }
            cnt++;
        }

        t.setField(0, new IntField(cnt));
        state = true;
        return t;
    }

    /**
     * @return return the children DbIterators of this operator. If there is
     * only one child, return an array of only one element. For join
     * operators, the order of the children is not important. But they
     * should be consistent among multiple calls.
     */
    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] opIterators = new OpIterator[]{this.child};
        return opIterators;
    }

    /**
     * Set the children(child) of this operator. If the operator has only one
     * child, children[0] should be used. If the operator is a join, children[0]
     * and children[1] should be used.
     *
     * @param children the DbIterators which are to be set as the children(child) of
     *                 this operator
     */
    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
        state = false;
    }
}
