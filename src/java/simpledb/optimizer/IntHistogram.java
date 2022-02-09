package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * the number of bucket
     */
    private final int bucketNum;

    /**
     * bucket Array
     */
    private int[] buckets;

    /**
     * Minutest value of bucket
     */
    private int min;

    /**
     * Maximum value of bucket
     */
    private int max;

    /**
     * width of each bucket
     */
    private double width;

    /**
     * numbers of Tuple
     */
    private int tupleNums = 0;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.bucketNum = buckets;
        this.min = min;
        this.max = max;
        this.buckets = new int[buckets];
        this.width = (1. + max - min) / this.buckets.length;
    }

    /**
     * get the index of v in the IntHistogram
     *
     * @param v target value
     * @return index in the IntHistogram
     */
    private int getIndex(int v) {
        if (v < this.min || v > this.max) throw new IllegalArgumentException("value {" + v + "} is illegal");
        return (int) ((v - this.min) / this.width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int i = getIndex(v);
        this.buckets[i]++;
        tupleNums++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        if (op.equals(Predicate.Op.LESS_THAN)) {
            if (v <= min) return 0.0;
            if (v >= max) return 1.0;
            final int index = getIndex(v);
            double cnt = 0;
            for (int i = 0; i < index; ++i) {
                cnt += buckets[i];
            }
            cnt += buckets[index] / width * (v - index * width - min);
            return cnt / tupleNums;
        }
        if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
        }
        if (op.equals(Predicate.Op.GREATER_THAN)) {
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        }
        if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
        }
        if (op.equals(Predicate.Op.EQUALS)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);
        }
        if (op.equals(Predicate.Op.NOT_EQUALS)) {
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return 0.0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        int cnt = 0;
        for (int bucket : buckets) cnt += bucket;

        return cnt / tupleNums;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("IntHistogram(buckets=%d, min=%d, max=%d", buckets.length, min, max);
    }
}
