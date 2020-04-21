package simpledb.file;

import simpledb.DbException;
import simpledb.util.IoUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * The SimpleDB file manager.
 * The database system stores its data as files within a specified directory.
 *
 * The file manager provides methods:
 * 1) reading the contents of a file block to a java byte buffer;
 * 2) writing the contents of a byte buffer to a file block;
 * 3) and appending the contents of a byte buffer to the end of a file.
 *
 * The class also contains public methods to indicate whether the
 * file is new, to give the number of blocks in the file, and
 * to give the number of bytes in each block.
 *
 * @author Edward Sciore
 */
public class FileMgr {

   private final Map<String, RandomAccessFile> openFiles = new HashMap<>();

   private final File dbDir;
   private final int blockSize;
   private final boolean isNew;

   /**
    * Creates a file manager for the specified database.
    * The database will be stored in a folder of that name
    * in the specified directory. Files for all temporary tables
    * (i.e. tables beginning with "temp") are deleted.
    *
    * @param dbDir the the directory that holds the database
    * @param blockSize the block size of buffer
    * @param isNew this db new or not
    *
    * @throws DbException if access db dir or delete temp files error
    */
   public FileMgr(File dbDir, int blockSize, boolean isNew) throws DbException {
      this.dbDir = dbDir;
      this.blockSize = blockSize;
      this.isNew = isNew;

      // Remove any leftover temporary tables
      String[] files = this.dbDir.list();
      if (files == null) {
         throw new DbException("Access db dir failed: " + this.dbDir);
      }
      for (String filename : files) {
         if (!filename.startsWith("temp")) {
            continue;
         }
         File file = new File(this.dbDir, filename);
         if (!file.delete()) {
            throw new DbException("Can't delete temp file: " + file);
         }
      }
   }

   /**
    * Reads the contents of a disk block into a byte array.
    * @param blk a reference to a disk block
    * @param p  the page
    */
   public void read(BlockId blk, Page p) throws DbException {
      try {
         RandomAccessFile raf = getFile(blk.fileName());
         synchronized (raf) {
            raf.seek(blk.number() * this.blockSize);
            FileChannel ch = raf.getChannel();
            ByteBuffer buffer = p.contents();

            while (buffer.hasRemaining()) {
               int n = ch.read(buffer);
               if (n == -1) {
                  break;
               }
            }
         }
      }
      catch (IOException e) {
         throw new DbException("Cannot read block " + blk, e);
      }
   }

   /**
    * Writes the contents of a byte array into a disk block.
    * @param blk a reference to a disk block
    * @param p  the page
    */
   public void write(BlockId blk, Page p) throws DbException {
      try {
         RandomAccessFile raf = getFile(blk.fileName());
         synchronized (raf) {
            raf.seek(blk.number() * this.blockSize);
            FileChannel ch = raf.getChannel();
            ByteBuffer buffer = p.contents();

            while (buffer.hasRemaining()) {
               ch.write(buffer);
            }
         }
      }
      catch (IOException e) {
         throw new DbException("Cannot write block" + blk, e);
      }
   }

   /**
    * Appends an empty block to the end of the specified file.
    * @param filename the name of the file
    * @return a reference to the newly-created block.
    */
   public BlockId append(String filename) {
      int newBlkNum = length(filename);
      BlockId blk = new BlockId(filename, newBlkNum);
      byte[] b = new byte[this.blockSize];

      try {
         RandomAccessFile raf = getFile(blk.fileName());
         synchronized (raf) {
            raf.seek(blk.number() * this.blockSize);
            raf.write(b);
         }
      }
      catch (IOException e) {
         throw new DbException("Cannot append block " + blk, e);
      }

      return blk;
   }

   /**
    * Returns the number of blocks in the specified file.
    * @param filename the name of the file
    * @return the number of blocks in the file
    */
   public int length(String filename) {
      try {
         RandomAccessFile raf = getFile(filename);
         synchronized (raf) {
            return (int) (raf.length() / this.blockSize);
         }
      }
      catch (IOException e) {
         throw new DbException("Cannot access file '" + filename + "'");
      }
   }

   /**
    * Returns a boolean indicating whether the file manager
    * had to create a new database directory.
    * @return true if the database is new
    */
   public boolean isNew() {
      return isNew;
   }
   
   public int blockSize() {
      return this.blockSize;
   }

   /**
    * Returns the file for the specified filename.
    * The file is stored in a map keyed on the filename.
    * If the file is not open, then it is opened and it
    * is added to the map.
    * @param filename the specified filename
    * @return the associated open file.
    * @throws IOException
    */
   private synchronized RandomAccessFile getFile(String filename) throws IOException {
      RandomAccessFile raf = this.openFiles.get(filename);

      if (raf == null) {
         File dbTable = new File(this.dbDir, filename);
         raf = new RandomAccessFile(dbTable, "rws");
         boolean failed = true;
         try {
            this.openFiles.put(filename, raf);
            failed = false;
         } finally {
            if (failed) {
               IoUtils.close(raf);
            }
         }
      }

      return raf;
   }

    public File getDbDir() {
      return this.dbDir;
    }

}
