package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		ArrayList<Tuple> tupleList = new ArrayList<>();
		for (Tuple tuple : this.tuples) {
			boolean isSelectedTuple = tuple.getField(field).compare(op, operand);
			if (isSelectedTuple) {
				tupleList.add(tuple);
			}
		}
		return new Relation(tupleList, this.td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		ArrayList<String> fieldNames = new ArrayList<>();
		ArrayList<Type> types = new ArrayList<>();
		for (int i = 0; i < this.td.numFields(); i++) {
			fieldNames.add(this.td.getFieldName(i));
			types.add(this.td.getType(i));
		}
	
		int index = 0;
		for (Integer field : fields) {
			fieldNames.set(field, names.get(index));
			index++;
		}
		
		ArrayList<Tuple> newTuples = this.tuples;
		TupleDesc newTd = new TupleDesc(types.toArray(new Type[types.size()]),fieldNames.toArray(new String[fieldNames.size()]));
		for (Tuple tuple : newTuples) {
			tuple.setDesc(newTd);
		}
		
		return new Relation(newTuples, newTd);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
	
		ArrayList<Tuple> newTuples = new ArrayList<>();
		ArrayList<Type> types = new ArrayList<>();
		ArrayList<String> tupleFields = new ArrayList<>();
		
		for (Integer field : fields) {
			types.add(this.td.getType(field));
			tupleFields.add(this.td.getFieldName(field));

		}

		TupleDesc newTd = new TupleDesc(types.toArray(new Type[types.size()]),tupleFields.toArray(new String[tupleFields.size()]));
		

		for (Tuple tuple : this.getTuples()) {

			Tuple newTuple = new Tuple(newTd);
			for(int i = 0; i < fields.size(); ++i) {
				newTuple.setField(i, tuple.getField(fields.get(i)));
			}
			newTuples.add(newTuple);
			
		}
		
		
		return new Relation(newTuples, newTd);
		
		
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		
		ArrayList<String> firstFields = new ArrayList<>();
		ArrayList<Type> firstTypes = new ArrayList<>();

		ArrayList<String> secondFields = new ArrayList<>();
		ArrayList<Type> secondTypes = new ArrayList<>();
		
		ArrayList<String> joinedFields = new ArrayList<>();
		ArrayList<Type> joinedTypes = new ArrayList<>();
		
		for (int i = 0; i < this.td.numFields(); i++) {
			firstFields.add(this.td.getFieldName(i));
			firstTypes.add(this.td.getType(i));
			joinedFields.add(this.td.getFieldName(i));
			joinedTypes.add(this.td.getType(i));
		
		}
		
		for (int i = 0; i < other.td.numFields(); i++) {
			secondFields.add(other.td.getFieldName(i));
			secondTypes.add(other.td.getType(i));
			joinedFields.add(other.td.getFieldName(i));
			joinedTypes.add(other.td.getType(i));

		}
		
		TupleDesc joinedTd = new TupleDesc(joinedTypes.toArray(new Type[joinedTypes.size()]), joinedFields.toArray(new String[joinedFields.size()]));
		ArrayList<Tuple> joinedTuples = new ArrayList<>();
	
		for (Tuple firstTuple : this.tuples) {
			for (Tuple secondTuple : other.tuples) {
				if(firstTuple.getField(field1).equals(secondTuple.getField(field2))) {
					Tuple newTuple = new Tuple(joinedTd);
					
				
					for (int i = 0; i < firstTuple.getDesc().numFields(); ++i) {
						newTuple.setField(i, firstTuple.getField(i));
					}
					for (int i = firstTuple.getDesc().numFields(); i < secondTuple.getDesc().numFields() + firstTuple.getDesc().numFields(); ++i) {
						newTuple.setField(i, firstTuple.getField(i-firstTuple.getDesc().numFields()));
					}
					
				
					joinedTuples.add(newTuple);
				}
			
			}
		}
		

		return new Relation(joinedTuples, joinedTd);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		
		//your code here
		Aggregator agg = new Aggregator(op, groupBy, td);
	
		for (Tuple t: this.tuples) {
			
			agg.merge(t);
		}
		ArrayList<Tuple> results = agg.getResults();
		Relation nr = new Relation(results, results.get(0).getDesc());
		return nr;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.td.toString());
		for (int i = 0; i< this.tuples.size(); ++i) {
			sb.append(this.tuples.get(i).toString());
		}
		return sb.toString();
		//your code here
	
	}
}
