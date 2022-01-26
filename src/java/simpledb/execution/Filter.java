package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * predicate
     */
    private final Predicate predicate;

    /**
     * operation Iterator
     */
    private OpIterator opIterator;

    private final TupleDesc tupleDesc;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.opIterator = child;
        this.tupleDesc = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    /**
     * @return return the TupleDesc of the output tuples of this operator
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * Open Operator Iterator
     *
     * @throws DbException
     * @throws NoSuchElementException
     * @throws TransactionAbortedException
     */
    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        super.open();
        opIterator.open();
    }

    /**
     * Closes this iterator. If overridden by a subclass, they should call
     * super.close() in order for Operator's internal state to be consistent.
     */
    public void close() {
        // some code goes here
        opIterator.close();
    }

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException           when rewind is unsupported.
     * @throws IllegalStateException If the iterator has not been opened
     */
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        opIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @throws NoSuchElementException      there are no more tuples
     * @throws TransactionAbortedException
     * @throws DbException
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        if (opIterator == null) {
            throw new NoSuchElementException("Operator Iterator is empty");
        }

        while (opIterator.hasNext()) {
            Tuple tuple = opIterator.next();
            if (predicate.filter(tuple)) return tuple;
        }

        return null;
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
        OpIterator[] opIterators = new OpIterator[]{this.opIterator};
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
        if (children == null || children.length == 0) return;

        opIterator = children[0];
    }

}
