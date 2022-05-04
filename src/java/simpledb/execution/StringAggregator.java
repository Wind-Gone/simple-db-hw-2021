package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private Type gbfieldType;
    private Op aggrOp;
    private final HashMap<Field, Integer> aggrMap = new HashMap<>();                           // IF GROUP, We arrange values by different field;
    private int ngroupValue;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.aggrOp = what;
        if (aggrOp != Op.COUNT)
            throw new IllegalArgumentException("what != COUNT");
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfieldType == null) {
            ngroupValue++;
        } else {
            Field field = tup.getField(gbfield);
            aggrMap.putIfAbsent(field, 0);
            aggrMap.put(field, aggrMap.get(field) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc tupleDesc;
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbfieldType == null) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(ngroupValue));
            tuples.add(tuple);
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE});
            for (Map.Entry<Field, Integer> fieldEntry : aggrMap.entrySet()) {
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, fieldEntry.getKey());
                tuple.setField(1, new IntField(fieldEntry.getValue()));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc, tuples);
    }
}
