package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private Type gbfieldType;
    private Op aggrOp;
    private final HashMap<Field, int[]> aggrMap = new HashMap<>();                           // IF GROUP, We arrange values by different field;
    private final int[] ngroupValue = new int[4];                                            // NO GROUP, We directly store values in all fields, 0: COUNT, 1: SUM, 2: MAX,3: MIN;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldType = gbfieldtype;
        this.aggrOp = what;
        if (gbfieldType == null) {
            ngroupValue[2] = Integer.MAX_VALUE;
            ngroupValue[3] = Integer.MIN_VALUE;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        int tupValue = ((IntField) tup.getField(afield)).getValue();
        if (gbfieldType == null) {
            ngroupValue[0]++;
            ngroupValue[1] += tupValue;
            ngroupValue[2] = Math.max(ngroupValue[2], tupValue);
            ngroupValue[3] = Math.min(ngroupValue[3], tupValue);
        } else {
            Field field = tup.getField(gbfield);
            aggrMap.putIfAbsent(field, new int[]{0, 0, Integer.MIN_VALUE, Integer.MAX_VALUE});
            int[] aggrMapValue = aggrMap.get(field);
            aggrMapValue[0]++;
            aggrMapValue[1] += tupValue;
            aggrMapValue[2] = Math.max(aggrMapValue[2], tupValue);
            aggrMapValue[3] = Math.min(aggrMapValue[3], tupValue);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    @SuppressWarnings("all")
    public OpIterator iterator() {
        // some code goes here
        TupleDesc tupleDesc;
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbfieldType == null) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple tuple = new Tuple(tupleDesc);
            switch (this.aggrOp) {
                case COUNT:
                    tuple.setField(0, new IntField(ngroupValue[0]));
                    break;
                case SUM:
                    tuple.setField(0, new IntField(ngroupValue[1]));
                    break;
                case MAX:
                    tuple.setField(0, new IntField(ngroupValue[2]));
                    break;
                case MIN:
                    tuple.setField(0, new IntField(ngroupValue[3]));
                    break;
                case AVG:
                    tuple.setField(0, new IntField(ngroupValue[1] / ngroupValue[0]));
                    break;
            }
            tuples.add(tuple);
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE});
            for (Map.Entry<Field, int[]> fieldEntry : aggrMap.entrySet()) {
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, fieldEntry.getKey());
                switch (this.aggrOp) {
                    case COUNT:
                        tuple.setField(1, new IntField(fieldEntry.getValue()[0]));
                        break;
                    case SUM:
                        tuple.setField(1, new IntField(fieldEntry.getValue()[1]));
                        break;
                    case MAX:
                        tuple.setField(1, new IntField(fieldEntry.getValue()[2]));
                        break;
                    case MIN:
                        tuple.setField(1, new IntField(fieldEntry.getValue()[3]));
                        break;
                    case AVG:
                        tuple.setField(1, new IntField(fieldEntry.getValue()[1] / fieldEntry.getValue()[0]));
                        break;
                }
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
