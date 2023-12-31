package hw1;
import java.util.*;
/*
 * Chae Hun Lim (441471)
 * Student 2 name:
 * Date: Oct 16 2023
 */
/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types;
	private String[] fields;
	
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	//your code here
    	this.types = typeAr;
    	this.fields = fieldAr;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        //your code here
    	return this.fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        //your code here
    	if (i > numFields() || i < 0) {
    		throw new NoSuchElementException();
    	}
    	return this.fields[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        //your code here
    	for (int i = 0; i < this.fields.length; ++i) {
    		if (this.fields[i].contentEquals(name)) {
    			return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        //your code here
    	if (i > this.types.length || i < 0) {
    		throw new NoSuchElementException();
    		
    	}
    	return this.types[i];

    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	//your code here
    	int size = 0;
    	for (Type type : this.types) {
    	
    		size += type.equals(Type.INT) ? 4 : 129;
    	}
    	return size;

    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	//your code here
    	if(o instanceof TupleDesc){
    		TupleDesc t = (TupleDesc)o;
    		if (this.getSize() != t.getSize()) {
    			return false;
    		}
    		for (int i = 0; i < this.types.length; ++i) {
    			if (this.types[i].STRING != t.types[i].STRING) {
    				return false;
    			}
    		}
    		return true;
    	
    	}
    
    	
    	return false;
    }
    

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer sb = new  StringBuffer();
        for (int i = 0; i < numFields(); ++i) {
        	sb.append(getType(i).toString() + "(" + getFieldName(i).toString() + "),"); 
        	
        }
        sb.deleteCharAt(sb.length()-1);
    	return sb.toString();
    }
}