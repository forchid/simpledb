package simpledb.plan;

import java.util.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.query.*;
import simpledb.record.*;

public class PlanTest2 {
   public static void main(String[] args) throws Exception {
      SimpleDB db = new SimpleDB("plantest2");
      Transaction tx = db.newTx();
      MetadataMgr mdm = db.mdMgr();
      
      Schema sch1 = new Schema();
      sch1.addIntField("A");
      sch1.addStringField("B", 9);
      mdm.createTable("T1", sch1, tx);
      Plan p1 = new TablePlan(tx, "T1", mdm);
      UpdateScan s1 = (UpdateScan) p1.open();
      s1.beforeFirst();
      int n = 200;
      System.out.println("Inserting " + n + " records into T1.");
      for (int i=0; i<n; i++) {
         s1.insert();
         s1.setInt("A", i);
         s1.setString("B", "bbb"+i);
      }
      s1.close();
      
      Schema sch2 = new Schema();
      sch2.addIntField("C");
      sch2.addStringField("D", 9);
      mdm.createTable("T2", sch2, tx);
      Plan p2 = new TablePlan(tx, "T2", mdm);
      UpdateScan s2 = (UpdateScan) p2.open();
      s2.beforeFirst();
      System.out.println("Inserting " + n + " records into T2.");
      for (int i=0; i<n; i++) {
         s2.insert();
         s2.setInt("C", n-i-1);
         s2.setString("D", "ddd"+(n-i-1));
      }
      s2.close();

      Plan p3 = new TablePlan(tx, "T1", mdm);
      Plan p4 = new TablePlan(tx, "T2", mdm);
      Plan p5 = new ProductPlan(p3, p4);
      // selecting all records where A=C
      Term t = new Term(new Expression("A"), new Expression("C")); 
      Predicate pred = new Predicate(t);
      System.out.println("The predicate is " + pred);
      Plan p6 = new SelectPlan(p5, pred);

      // projecting on [B,D]
      List<String> c = Arrays.asList("B", "D");
      Plan p7 = new ProjectPlan(p6, c);
      Scan s7 = p7.open();
      while (s7.next())
         System.out.println(s7.getString("B") + " " + s7.getString("D")); 
      s7.close();
      tx.commit();
   }
}
