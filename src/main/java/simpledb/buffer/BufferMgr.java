package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 *
 * @author Edward Sciore
 *
 */
public class BufferMgr {

   private final Buffer[] bufferPool;
   private int numAvailable;
   private static final long MAX_TIME = 10000; // 10 seconds
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on a {@link FileMgr} and
    * {@link LogMgr LogMgr} object.
    * @param bufferNum the number of buffer slots to allocate
    */
   public BufferMgr(FileMgr fm, LogMgr lm, int bufferNum) {
      this.bufferPool = new Buffer[bufferNum];
      this.numAvailable = bufferNum;
      for (int i = 0; i < bufferNum; i++) {
         this.bufferPool[i] = new Buffer(fm, lm);
      }
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   public synchronized int available() {
      return numAvailable;
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txId the transaction's id number
    */
   public synchronized void flushAll(int txId) {
      for (Buffer buff : this.bufferPool) {
         if (buff.modifyingTx() == txId) {
            buff.flush();
         }
      }
   }
   
   
   /**
    * Unpins the specified data buffer. If its pin count
    * goes to zero, then notify any waiting threads.
    * @param buffer the buffer to be unpinned
    */
   public synchronized void unpin(Buffer buffer) {
      buffer.unpin();
      if (!buffer.isPinned()) {
         this.numAvailable++;
         notifyAll();
      }
   }
   
   /**
    * Pins a buffer to the specified block, potentially
    * waiting until a buffer becomes available.
    * If no buffer becomes available within a fixed 
    * time period, then a {@link BufferAbortException} is thrown.
    * @param blk a reference to a disk block
    * @return the buffer pinned to that block
    */
   public synchronized Buffer pin(BlockId blk) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = tryToPin(blk);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = tryToPin(blk);
         }
         if (buff == null) {
            throw new BufferAbortException();
         }
         return buff;
      }
      catch(InterruptedException e) {
         throw new BufferAbortException();
      }
   }  
   
   private boolean waitingTooLong(long startTime) {
      return System.currentTimeMillis() - startTime > MAX_TIME;
   }
   
   /**
    * Tries to pin a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   private Buffer tryToPin(BlockId blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
      }
      if (!buff.isPinned()) {
         this.numAvailable--;
      }
      buff.pin();
      return buff;
   }
   
   private Buffer findExistingBuffer(BlockId blk) {
      for (Buffer buff : this.bufferPool) {
         BlockId b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }

      return null;
   }
   
   private Buffer chooseUnpinnedBuffer() {
      for (Buffer buff : this.bufferPool) {
         if (!buff.isPinned()) {
            return buff;
         }
      }

      return null;
   }

}
