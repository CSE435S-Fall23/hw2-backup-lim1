package hw1;
/*
 * Chae Hun Lim (441471)
 * Student 2 name:
 * Date: Oct 16 2023
 */
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		Catalog catalog = Database.getCatalog();
		String tbName = sb.getFromItem().toString();


		int id = catalog.getTableId(tbName);
		TupleDesc td = catalog.getTupleDesc(id);
		ArrayList<Tuple> tuples = catalog.getDbFile(id).getAllTuples();
		Relation rel = new Relation(tuples, td);
		
		
		//join, where, select
		List<Join> joins = sb.getJoins();
		Expression where = sb.getWhere();
		List<SelectItem> selects = sb.getSelectItems();
		
		
		if (joins != null) {
	
			for (Join j : joins) {
				Table r = (Table) j.getRightItem();
				String name = r.getName();

				int jId = catalog.getTableId(name);
				
				TupleDesc jTd = catalog.getTupleDesc(jId);
				ArrayList<Tuple> jTuples = catalog.getDbFile(jId).getAllTuples();
				Relation jR = new Relation(jTuples, jTd);
			
				
				Expression ex = j.getOnExpression();
				EqualsTo eq = (EqualsTo)ex;
				Expression rE = eq.getRightExpression();
				Expression lE = eq.getLeftExpression();
	
				Column rC = (Column)rE;
				Column lC = (Column)lE;
				
				int lId = rel.getDesc().nameToId(lC.getColumnName());
				int rId = jR.getDesc().nameToId(rC.getColumnName());
			
				rel = rel.join(jR, lId, rId);
				
				
			}
		}
		if(where != null) {
			WhereExpressionVisitor w = new WhereExpressionVisitor();
			where.accept(w);
			int left = rel.getDesc().nameToId(w.getLeft()); 
			
			rel = rel.select(left, w.getOp(), w.getRight());
			
		}
		
		
		
		
		rel = select(selects, rel, sb);
	
		return rel;
		
	}
	
	public Relation select(List<SelectItem> selects, Relation rel, PlainSelect sb) {
		
		AggregateOperator op = null;
		ArrayList<Integer> renameId = new ArrayList<>();
		ArrayList<String> renameNames = new ArrayList<>();
		
		TupleDesc relTd = rel.getDesc();
		
		ArrayList<Integer> indices = new ArrayList<>();
	
		for (SelectItem i : selects) {
			ColumnVisitor cv = new ColumnVisitor();
			i.accept(cv);
			if (cv.getColumn().equals("*")) {
				for (int t = 0; t<relTd.numFields();++t) {
					indices.add(t);
				}
			
			}
			else {
				indices.add(rel.getDesc().nameToId(cv.getColumn()));

				
				
			}
			if(i.toString() == ("AS")) {
				
				
				renameId.add(relTd.nameToId(cv.getColumn()));
				renameNames.add(cv.getColumn());
				rel = rel.rename(renameId, renameNames);
			}
			
				
			
			if(cv.isAggregate()) {
				op = cv.getOp();
			}
		}
		
			
		rel = rel.project(indices);
		
	
	Boolean groupBy = (sb.getGroupByColumnReferences() != null);

	if (op != null) {

		rel = rel.aggregate(op, groupBy);
	}
	return rel;
		
	}
}
