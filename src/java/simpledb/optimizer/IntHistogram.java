package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int buckets;                // the number of buckets
    private int minVal;                 // the min value of all tuples
    private int maxVal;                 // the max value in all tuples
    private int[] bucketArray;          // the stored value
    private double width;               // the width of histogram
    private int ntuples;                // the number of all tuples in the table

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
        this.buckets = buckets;
        this.maxVal = max;
        this.minVal = min;
        this.bucketArray = new int[buckets];
        this.width = (double) (maxVal - minVal) / buckets;
        this.ntuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v >= minVal && v <= maxVal) {
            int bucketIndex = v == maxVal ? buckets - 1 : (int) ((v - minVal) / width);
            ntuples++;
            bucketArray[bucketIndex]++;
        }
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
        double selectivity = 0.0;
        switch (op) {
            case EQUALS:
                if (v >= minVal && v <= maxVal) {
                    int bucketIndex = v == maxVal ? buckets - 1 : (int) ((v - minVal) / width);
//                    return (bucketArray[bucketIndex] / width) / ntuples;
                    return (double) bucketArray[bucketIndex] / (width + 1) / ntuples;
                }
                else
                    return selectivity;
            case GREATER_THAN:
                if (v <= minVal)
                    selectivity = 1.0;
                else if (v >= maxVal)
                    selectivity = 0.0;
                else {
                    int bucketIndex = (int) Math.ceil((v - minVal) / width);
                    for (int i = bucketIndex + 1; i < buckets; i++)
                        selectivity += (double) bucketArray[i] / ntuples;
                    double h_b = bucketArray[bucketIndex];
                    double b_f = h_b / ntuples;
                    double b_right = minVal + (bucketIndex + 1) * width - 1;
                    double b_part = (b_right - v + 1) / width;
                    selectivity += b_part * b_f;
                }
                return selectivity;
            case LESS_THAN:
                return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                break;
        }
        return selectivity;
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
        double avgSelectivity = 0.0;
        for (int i = 0; i < buckets; i++)
            avgSelectivity += (double) bucketArray[i] / ntuples;
        return avgSelectivity;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder histDescription = new StringBuilder();
        histDescription.append("IntHistogram's value range is [").append(minVal).append(", ").append(maxVal).append("] and it has").append(bucketArray.length).append("buckets\n");
        for (int i = 0; i < bucketArray.length; ++i) {
            int rangeLeft = (int) (minVal + i * width);
            int rangeRight = (width < 1.0) ? rangeLeft : (int) (rangeLeft + width - 1);
            histDescription.append("[").append(rangeLeft).append(", ").append(rangeRight).append("]:").append(bucketArray[i]).append("\n");
        }
        return histDescription.toString();
    }
}
