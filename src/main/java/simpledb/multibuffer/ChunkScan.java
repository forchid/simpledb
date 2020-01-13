package simpledb.multibuffer;

import static java.sql.Types.INTEGER;
import java.util.*;
import simpledb.file.BlockId;
import simpledb.query.*;
import simpledb.tx.Transaction;
import simpledb.record.*;

/**
 * The class for the <i>chunk</i> operator.
 * @author Edward Sciore
 */
public class ChunkScan implements Scan {
   private List<RecordPage> buffs = new ArrayList<>();
   private Transaction tx;
   private String filename;
   private Layout layout;
   private int startbnum, endbnum, currentbnum;
   private RecordPage rp;
   private int currentslot;

   /**
    * Create a chunk consisting of the specified pages. 
    * @param layout the metadata for the chunked table
    * @param startbnum the starting block number
    * @param endbnum  the ending block number
    * @param tx the current transaction
    */ 
   public ChunkScan(Transaction tx, String filename, Layout layout, int startbnum, int endbnum) {
      this.tx = tx;
      this.filename = filename;
      this.layout = layout;
      this.startbnum = startbnum;
      this.endbnum   = endbnum;
      for (int i=startbnum; i<=endbnum; i++) {
         BlockId blk = new BlockId(filename, i);
         buffs.add(new RecordPage(tx, blk, layout));
      }
      moveToBlock(startbnum);
   }

   /**
    * @see Scan#close()
    */
   public void close() {
      for (int i=0; i<buffs.size(); i++) {
         BlockId blk = new BlockId(filename, startbnum+i);
         tx.unpin(blk);
      }
   }

   /**
    * @see Scan#beforeFirst()
    */
   public void beforeFirst() {
      moveToBlock(startbnum);
   }

   /**
    * Moves to the next record in the current block of the chunk.
    * If there are no more records, then make
    * the next block be current.
    * If there are no more blocks in the chunk, return false.
    * @see Scan#next()
    */
   public boolean next() {
      currentslot = rp.nextAfter(currentslot);
      while (currentslot < 0) {
         if (currentbnum == endbnum)
            return false;
         moveToBlock(rp.block().number()+1);
         currentslot = rp.nextAfter(currentslot);
      }
      return true;
   }

   /**
    * @see Scan#getInt(String)
    */
   public int getInt(String fldname) {
      return rp.getInt(currentslot, fldname);
   }

   /**
    * @see Scan#getString(String)
    */
   public String getString(String fldname) {
      return rp.getString(currentslot, fldname);
   }

   /**
    * @see Scan#getVal(String)
    */
   public Constant getVal(String fldname) {
      if (layout.schema().type(fldname) == INTEGER)
         return new Constant(getInt(fldname));
      else
         return new Constant(getString(fldname));
   }

  /**
    * @see Scan#hasField(String)
    */
   public boolean hasField(String fldname) {
      return layout.schema().hasField(fldname);
   }

   private void moveToBlock(int blknum) {
      currentbnum = blknum;
      rp = buffs.get(currentbnum - startbnum);
      currentslot = -1;
   }
}