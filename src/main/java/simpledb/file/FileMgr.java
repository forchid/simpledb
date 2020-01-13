package simpledb.file;

import java.io.*;
import java.util.*;

/**
 * The SimpleDB file manager.
 * The database system stores its data as files within a specified directory.
 * The file manager provides methods for reading the contents of
 * a file block to a Java byte buffer,
 * writing the contents of a byte buffer to a file block,
 * and appending the contents of a byte buffer to the end of a file.
 * The class also contains public methods to indicate whether the
 * file is new, to give the number of blocks in the file, and
 * to give the number of bytes in each block.
 * @author Edward Sciore
 */
public class FileMgr {
   private File dbDirectory;
   private int blocksize;
   private boolean isNew;
   private Map<String,RandomAccessFile> openFiles = new HashMap<>();

   /**
    * Creates a file manager for the specified database.
    * The database will be stored in a folder of that name
    * in the specified directory. If the folder does not exist,
    * then a folder containing an empty database is created 
    * automatically. Files for all temporary tables 
    * (i.e. tables beginning with "temp") are deleted.
    * @param dbname the name of the directory that holds the database
    */
   public FileMgr(File dbDirectory, int blocksize) {
      this.dbDirectory = dbDirectory;
      this.blocksize = blocksize;
      isNew = !dbDirectory.exists();

      // create the directory if the database is new
      if (isNew)
         dbDirectory.mkdirs();

      // remove any leftover temporary tables
      for (String filename : dbDirectory.list())
         if (filename.startsWith("temp"))
         		new File(dbDirectory, filename).delete();
   }

   /**
    * Reads the contents of a disk block into a byte array.
    * @param blk a reference to a disk block
    * @param p  the page
    */
   public synchronized void read(BlockId blk, Page p) {
      try {
         RandomAccessFile f = getFile(blk.fileName());
         f.seek(blk.number() * blocksize);
         f.getChannel().read(p.contents());
      }
      catch (IOException e) {
         throw new RuntimeException("cannot read block " + blk);
      }
   }

   /**
    * Writes the contents of a byte array into a disk block.
    * @param blk a reference to a disk block
    * @param p  the page
    */
   public synchronized void write(BlockId blk, Page p) {
      try {
         RandomAccessFile f = getFile(blk.fileName());
         f.seek(blk.number() * blocksize);
         f.getChannel().write(p.contents());
      }
      catch (IOException e) {
         throw new RuntimeException("cannot write block" + blk);
      }
   }

   /**
    * Appends an empty block to the end of the specified file.
    * @param filename the name of the file
    * @return a reference to the newly-created block.
    */
   public synchronized BlockId append(String filename) {
      int newblknum = length(filename);
      BlockId blk = new BlockId(filename, newblknum);
      byte[] b = new byte[blocksize];
      try {
         RandomAccessFile f = getFile(blk.fileName());
         f.seek(blk.number() * blocksize);
         f.write(b);
      }
      catch (IOException e) {
         throw new RuntimeException("cannot append block" + blk);
      }

      return blk;
   }

   /**
    * Returns the number of blocks in the specified file.
    * @param filename the name of the file
    * @return the number of blocks in the file
    */
   public synchronized int length(String filename) {
      try {
         RandomAccessFile f = getFile(filename);
         return (int)(f.length() / blocksize);
      }
      catch (IOException e) {
         throw new RuntimeException("cannot access " + filename);
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
      return blocksize;
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
   private RandomAccessFile getFile(String filename) throws IOException {
      RandomAccessFile f = openFiles.get(filename);
      if (f == null) {
         File dbTable = new File(dbDirectory, filename);
         f = new RandomAccessFile(dbTable, "rws");
         openFiles.put(filename, f);
      }
      return f;
   }
}
