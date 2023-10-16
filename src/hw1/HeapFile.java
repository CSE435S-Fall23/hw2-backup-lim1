package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
/*
 * Chae Hun Lim (441471)
 * Student 2 name:
 * Date: Oct 16 2023
 */
/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	private File file;
	private TupleDesc td;
	private int id;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		//your code here
		this.file = f;
		this.td = type;
		this.id = file.hashCode();
	}
	
	public File getFile() {
		//your code here
		return this.file;

	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.td;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here

		try {
			RandomAccessFile raf = new RandomAccessFile(this.file, "r");
			byte[] dataBytes = new byte[PAGE_SIZE]; 
			raf.seek(PAGE_SIZE * id);
			raf.read(dataBytes);
			raf.close();
			HeapPage page = new HeapPage(id, dataBytes, getId());
			return page;
			
		}
		catch(IOException e){
			e.printStackTrace();
			
		}
		
		return null;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return this.id;
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		//your code here
		try {
			RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
			raf.seek(PAGE_SIZE * p.getId()); 
			raf.write(p.getPageData());
			raf.close();
			
			
		}
		catch(IOException e){
			e.printStackTrace();
			
		}
		
		
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 * @throws Exception 
	 */
	public HeapPage addTuple(Tuple t) throws Exception {
	
		//your code here

		int numPages = this.getNumPages();
		for (int i = 0; i < numPages; ++i) {
			HeapPage curPage = this.readPage(i);
			int numSlots = curPage.getNumSlots();
		
			for(int j = 0; j < numSlots; ++j) {
				if (!curPage.slotOccupied(j)) {
				
					
						curPage.addTuple(t);
						this.writePage(curPage);
						return curPage;
				
				}
			
			}
			
		}
	
		HeapPage page = new HeapPage(numPages, new byte[PAGE_SIZE], getId());
		page.addTuple(t);
		this.writePage(page);
		return page;
			
			
		
		
	
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		try {
			
			HeapPage targetPage = this.readPage(t.getId());
			targetPage.deleteTuple(t);
			this.writePage(targetPage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> allTuplesList = new ArrayList<>();
		for(int i = 0; i < getNumPages(); ++i) {
			HeapPage curPage = this.readPage(i);
			Iterator<Tuple> tupleIterator = curPage.iterator();
		
			while (tupleIterator.hasNext()) {
				Tuple tuple = tupleIterator.next();

				allTuplesList.add(tuple);
			
			}
		}
		
		return allTuplesList;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		
		return (int)(this.file.length() / PAGE_SIZE);
	}
}
