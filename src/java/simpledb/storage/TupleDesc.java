package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private final ArrayList<TDItem> tupleDescArr;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        assert typeAr.length == fieldAr.length;
        tupleDescArr = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            tupleDescArr.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tupleDescArr = new ArrayList<>();
        for (Type type : typeAr) {
            tupleDescArr.add(new TDItem(type));
        }
    }

    @SuppressWarnings("all")
    private static final long serialVersionUID = 1L;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    @SuppressWarnings("all")
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        ArrayList<Type> mergedTupleDescType = new ArrayList<>();             // merged types
        ArrayList<String> mergedTupleDescNames = new ArrayList<>();          // merged field names
        for (TDItem tupleDesc1 : td1.tupleDescArr) {
            mergedTupleDescType.add(tupleDesc1.fieldType);
            mergedTupleDescNames.add(tupleDesc1.fieldName);
        }
        for (TDItem tupleDesc2 : td2.tupleDescArr) {
            mergedTupleDescType.add(tupleDesc2.fieldType);
            mergedTupleDescNames.add(tupleDesc2.fieldName);
        }
        return new TupleDesc(
                mergedTupleDescType.toArray(new Type[mergedTupleDescType.size()]),
                mergedTupleDescNames.toArray(new String[mergedTupleDescNames.size()]));
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tupleDescArr.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < tupleDescArr.size())
            return tupleDescArr.get(i).fieldName;
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < tupleDescArr.size())
            return tupleDescArr.get(i).fieldType;
        throw new NoSuchElementException();
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < tupleDescArr.size(); i++)
            if (tupleDescArr.get(i).fieldName.equalsIgnoreCase(name))
                return i;
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalSize = 0;
        for (TDItem item : tupleDescArr)
            totalSize += item.fieldType.getLen();
        return totalSize;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == null || o.getClass() != TupleDesc.class)
            return false;
        try {
            TupleDesc tupleDesc = (TupleDesc) o;
            if (this.numFields() != tupleDesc.numFields() || this.getSize() != tupleDesc.getSize())
                return false;
            for (int i = 0; i < tupleDesc.numFields(); i++) {
                if (!tupleDesc.tupleDescArr.get(i).fieldName.equals(tupleDescArr.get(i).fieldName) || !tupleDesc.tupleDescArr.get(i).fieldType.equals(tupleDescArr.get(i).fieldType))
                    return false;
            }
            return true;
        } catch (ClassCastException e) {
            return false;
        }
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder outputStr = new StringBuilder();
        for (TDItem tdItem : tupleDescArr) {
            outputStr.append(tdItem.toString());
            outputStr.append(",");
        }
        return outputStr.toString();
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {
        @SuppressWarnings("all")
        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        /**
         * Add Constructor for anonymous field
         */
        public TDItem(Type t) {
            this.fieldType = t;
            this.fieldName = "anonymous";
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
}
