package test;


import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import hw1.AggregateOperator;
import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.IntField;
import hw1.Relation;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;
/*
 * Chae Hun Lim (441471)
 * Student 2 name:
 * Date: Sep 22 2023
 */
public class YourUnitTestsHW2 {
	
	private HeapFile hf;
	private TupleDesc td;
	private Catalog c;
	private HeapPage hp;
	private HeapFile ahf;
	private TupleDesc atd;

	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		td = c.getTupleDesc(tableId);
		hf = c.getDbFile(tableId);
		hp = hf.readPage(0);
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
		
		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
	}
	
	@Test
	public void testAvg() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.AVG, false);
		
		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField)(ar.getTuples().get(0).getField(0));

		assertTrue(agg.getValue() == 4);
	}
	
	@Test
	public void testMin() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.MIN, false);
		
		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField)(ar.getTuples().get(0).getField(0));
	
		assertTrue(agg.getValue() == 1);
	}



}