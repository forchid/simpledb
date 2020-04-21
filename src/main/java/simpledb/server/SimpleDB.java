package simpledb.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import simpledb.DbException;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.buffer.BufferMgr;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.*;
import simpledb.index.planner.IndexUpdatePlanner;
import simpledb.opt.HeuristicQueryPlanner;
import simpledb.util.IoUtils;

/**
 * The class that configures the system.
 * 
 * @author Edward Sciore
 */
public class SimpleDB {

   static final String PROP_BUFFER_SIZE = "simpledb.server.bufferSize";
   static final String PROP_ERROR_FILE = "simpledb.server.errorFile";

   public static final int BLOCK_SIZE  = 4 << 10;
   public static final int BUFFER_SIZE = Integer.getInteger(PROP_BUFFER_SIZE, 8 << 10);
   public static final String LOG_FILE = "wal.log";

   static final String ERROR_FILE = System.getProperty(PROP_ERROR_FILE);

   private final PrintStream errorLog;

   private final FileMgr     fm;
   private final BufferMgr   bm;
   private final LogMgr      lm;
   private  MetadataMgr mdm;
   private  Planner planner;

   /**
    * A constructor useful for debugging.
    * @param dirname the name of the database directory
    * @param blockSize the block size
    * @param bufferSize the number of buffers
    */
   public SimpleDB(String dirname, int blockSize, int bufferSize) throws DbException {
      File dbDir = new File(dirname);
      boolean isNew = false;
      if (!dbDir.isDirectory()) {
         if (!dbDir.mkdirs()) {
            throw new DbException("Can't create db dir '" + dbDir + "'");
         }
         isNew = true;
      }

      if (ERROR_FILE == null) {
         this.errorLog = System.out;
      } else {
         File errorFile = new File(ERROR_FILE);
         if (!errorFile.isAbsolute()) {
            errorFile = new File(dbDir, ERROR_FILE);
         }
         try {
            this.errorLog = new PrintStream(errorFile);
         } catch (FileNotFoundException e) {
            throw new DbException("Can't create error file", e);
         }
      }

      this.fm = new FileMgr(dbDir, blockSize, isNew);
      this.lm = new LogMgr(this.fm, LOG_FILE);
      this.bm = new BufferMgr(this.fm, lm, bufferSize);
   }
   
   /**
    * A simpler constructor for most situations. Unlike the
    * 3-arg constructor, it also initializes the metadata tables.
    * @param dirname the name of the database directory
    */
   public SimpleDB(String dirname) {
      this(dirname, BLOCK_SIZE, BUFFER_SIZE);
      File dbDir = this.fm.getDbDir().getAbsoluteFile();

      Transaction tx = newTx();
      boolean isNew = this.fm.isNew();
      if (isNew) {
         info("Creating new database in dir '%s'", dbDir);
      } else {
         info("Recovering existing database in dir '%s'", dbDir);
         tx.recover();
      }
      this.mdm = new MetadataMgr(isNew, tx);
//    QueryPlanner qp = new HeuristicQueryPlanner(mdm);
      QueryPlanner qp = new BasicQueryPlanner(this.mdm);
//    UpdatePlanner up = new IndexUpdatePlanner(mdm);
      UpdatePlanner up = new BasicUpdatePlanner(this.mdm);
      this.planner = new Planner(qp, up);
      tx.commit();
   }
   
   /**
    * A convenient way for clients to create transactions
    * and access the metadata.
    */
   public Transaction newTx() {
      return new Transaction(this, fm, lm, bm);
   }
   
   public MetadataMgr mdMgr() {
      return mdm;
   }
   
   public Planner planner() {
      return planner;
   }

   // These methods aid in debugging
   public FileMgr fileMgr() {
      return fm;
   }

   public LogMgr logMgr() {
      return lm;
   }

   public BufferMgr bufferMgr() {
      return bm;
   }

   public void debug(String format, Object... args) {
      IoUtils.debug(this.errorLog, format, args);
   }

   public void info(String format, Object... args) {
      IoUtils.info(this.errorLog, format, args);
   }

   public void error(String format, Object... args) {
      IoUtils.error(this.errorLog, format, args);
   }

   public void error(Throwable cause, String format, Object... args) {
      IoUtils.error(this.errorLog, cause, format, args);
   }

 }
