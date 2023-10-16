package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
/*
 * Chae Hun Lim (441471)
 * Student 2 name:
 * Date: Oct 16 2023
 */

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	AggregateOperator aggOp;
	boolean groupBy;
	TupleDesc td;
	ArrayList<Tuple> results;
	ArrayList<Tuple> single;
	
	ArrayList<HashMap<Field, ArrayList<Tuple>>> columns;

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.aggOp = o;
		this.groupBy = groupBy;
		this.td = td;
		this.columns = new ArrayList<>();
		HashMap<Field, ArrayList<Tuple>> groupMap = new HashMap<>();
		HashMap<Field, ArrayList<Tuple>> singleMap = new HashMap<>();
		this.columns.add(singleMap);
		this.columns.add(groupMap);
		this.results = new ArrayList<>();
		this.single = new ArrayList<>();


	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		HashMap<Field, ArrayList<Tuple>> gm = this.columns.get(1);
		
		
		if (groupBy) {
			Field k = t.getField(0);
			addTuple(gm,t,k);
			return;
		}
		else {
		
			this.single.add(t);
			return;

			
		}
		
		
	}
	
	public void addTuple(HashMap<Field, ArrayList<Tuple>> m, Tuple t, Field k) {
		if(m.containsKey(k)) {
			m.get(k).add(t);
		}
		else {
			ArrayList<Tuple> newGroup = new ArrayList<>();
			newGroup.add(t);
			m.put(k, newGroup);
		}
		
	}
	
	
	
	public ArrayList<Tuple> getGroupBy(){
		HashMap<Field, ArrayList<Tuple>> gm = this.columns.get(1);
	
		switch(this.aggOp) {
		case MAX:
			
			for(Entry<Field, ArrayList<Tuple>> entry : gm.entrySet()) {
				Tuple curMax = entry.getValue().get(0);
				for(Tuple gT : entry.getValue()) {
					if(gT.getField(1).compare(RelationalOperator.GT, curMax.getField(1))) {
						curMax = gT;
					}
				}
				this.results.add(curMax);
		}
			return this.results;
		case MIN:
			for(Entry<Field, ArrayList<Tuple>> entry : gm.entrySet()) {
				Tuple curMax = entry.getValue().get(0);
				for(Tuple gT : entry.getValue()) {
					if(gT.getField(1).compare(RelationalOperator.LT, curMax.getField(1))) {
						curMax = gT;
					}
				}
				this.results.add(curMax);
		}
			return this.results;
		case AVG:
			if (this.td.getType(0) == Type.STRING) {
				this.results = null;
			}
			else {
				for(Entry<Field, ArrayList<Tuple>> entry : gm.entrySet()) {
					int avg_ = 0;
					int n = 0;
					for(Tuple gT : entry.getValue()) {
						IntField curField = (IntField)gT.getField(1);
						int curVal = curField.getValue();
						avg_ += curVal;
						n += 1;
					}
					Type[] newType = new Type[2];
					newType[0] = this.td.getType(0);
					newType[1] = Type.INT;
					
					String[] newName = new String[2];
					newName[0] = this.td.getFieldName(0);
					newName[1] = "VAL";
					
					TupleDesc newTd = new TupleDesc(newType, newName);
					Tuple sumTuple = new Tuple(newTd);
					sumTuple.setField(0, entry.getKey());
			
					sumTuple.setField(1, new IntField(avg_/n));
					this.results.add(sumTuple);
			
				
				
				}
				
				
			}
			return this.results;
		
		case COUNT:
			if (this.td.getType(0) == Type.STRING) {
				this.results = null;
			}
			else {
				for(Entry<Field, ArrayList<Tuple>> entry : gm.entrySet()) {
				
					int n = entry.getValue().size();
					
					Type[] newType = new Type[2];
					newType[0] = this.td.getType(0);
					newType[1] = Type.INT;
					
					String[] newName = new String[2];
					newName[0] = this.td.getFieldName(0);
					newName[1] = "VAL";
					
					TupleDesc newTd = new TupleDesc(newType, newName);
					Tuple sumTuple = new Tuple(newTd);
					sumTuple.setField(0, entry.getKey());
			
					sumTuple.setField(1, new IntField(n));
					this.results.add(sumTuple);
			
				
				
				}
				
				
			}
			return this.results;
		
		case SUM: 
			
			if (this.td.getType(0) == Type.STRING) {
				this.results = null;
			}
			else {
			
				for(Entry<Field, ArrayList<Tuple>> entry : gm.entrySet()) {
					int sum_ = 0;
					for(Tuple gT : entry.getValue()) {
						IntField curField = (IntField)gT.getField(1);
						int curVal = curField.getValue();
						sum_ += curVal;
						
					}
					Type[] newType = new Type[2];
					newType[0] = this.td.getType(0);
					newType[1] = Type.INT;
					
					String[] newName = new String[2];
					newName[0] = this.td.getFieldName(0);
					newName[1] = "VAL";
					
					TupleDesc newTd = new TupleDesc(newType, newName);
					Tuple sumTuple = new Tuple(newTd);
					sumTuple.setField(0, entry.getKey());
			
					sumTuple.setField(1, new IntField(sum_));
					this.results.add(sumTuple);
			
				
				
				}
			}
			return this.results;
			
			
		
		default:
			throw new IllegalArgumentException();
			
			
		
		
		}
		
	}
	
	public ArrayList<Tuple> getAggregateSingle(){
		switch(this.aggOp) {
		case SUM:
		if (this.td.getType(0) == Type.STRING) {
			return null;
		}
		else {
			
			Type[] newType = new Type[1];
			newType[0] = this.td.getType(0);
			String[] newName = new String[1];
			newName[0] = "VAL";
			TupleDesc newTd = new TupleDesc(newType, newName);
			Tuple sumTuple = new Tuple(newTd);
			int sum_ = 0;
			for (Tuple gt : this.single) {

				IntField curField = (IntField)gt.getField(0);
				int curVal = curField.getValue();
				sum_ += curVal;
			}
		
			sumTuple.setField(0, new IntField(sum_));
			this.results.add(sumTuple);
			return this.results;
		
		}
		
		case MAX:
			Tuple curMax = this.single.get(0);
			for(Tuple gT : this.single) {
				if(gT.getField(0).compare(RelationalOperator.GT, curMax.getField(0))) {
					curMax = gT;
				}
			}
			this.results.add(curMax);
			return this.results;
		
		case MIN:
			Tuple curMin = this.single.get(0);
			for(Tuple gT : this.single) {
				if(gT.getField(0).compare(RelationalOperator.LT, curMin.getField(0))) {
					curMax = gT;
				}
			}
			this.results.add(curMin);
			return this.results;
		
		case COUNT:
			if (this.td.getType(0) == Type.STRING) {
				return null;
			}
			else {
				
				Type[] newType = new Type[1];
				newType[0] = this.td.getType(0);
				String[] newName = new String[1];
				newName[0] = "VAL";
				TupleDesc newTd = new TupleDesc(newType, newName);
				Tuple sumTuple = new Tuple(newTd);
				int n = this.single.size();
				
				
			
				sumTuple.setField(0, new IntField(n));
				this.results.add(sumTuple);
				return this.results;
			
			}
		
		case AVG:
			if (this.td.getType(0) == Type.STRING) {
				return null;
			}
			else {
				
				Type[] newType = new Type[1];
				newType[0] = this.td.getType(0);
				String[] newName = new String[1];
				newName[0] = "VAL";
				TupleDesc newTd = new TupleDesc(newType, newName);
				Tuple sumTuple = new Tuple(newTd);
				int avg_ = 0;
				int n = 0;
				for (Tuple gt : this.single) {
					n += 1;
					IntField curField = (IntField)gt.getField(0);
					int curVal = curField.getValue();
					avg_ += curVal;
				}
			
				sumTuple.setField(0, new IntField(avg_/n));
				this.results.add(sumTuple);
				return this.results;
			
			}
		default:
			throw new IllegalArgumentException();
		}
		
	}
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		if(this.groupBy) {
			return getGroupBy();
		}
		else {
			return getAggregateSingle();

		}
	
	}

}
