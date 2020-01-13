package simpledb.plan;

import java.util.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.query.*;


import simpledb.record.*;

public class PlanTest1 {
   public static void main(String[] args) {
      SimpleDB db = new SimpleDB("plantest1");
      Transaction tx = db.newTx();
      MetadataMgr mdm = db.mdMgr();

      Schema sch = new Schema();
      sch.addIntField("A");
      sch.addStringField("B", 9);
      mdm.createTable("T", sch, tx);
      Plan p1 = new TablePlan(tx, "T", mdm);
      UpdateScan s1 = (UpdateScan)p1.open();
      s1.beforeFirst();
      int n = 200;
      System.out.println("Inserting " + n + " random records.");
      for (int i=0; i<n; i++) {
         s1.insert();
         int k = (int) Math.round(Math.random() * 50);
         s1.setInt("A", k);
         s1.setString("B", "rec"+k);
      }
      s1.close();

      Plan p2 = new TablePlan(tx, "T", mdm);
      // selecting all records where A=10
      Term term = new Term(new Expression("A"), new Expression(new Constant(10)));
      Predicate pred = new Predicate(term); 
      Plan p3 = new SelectPlan(p2, pred);

      // projecting on B
      List<String> c = Arrays.asList("B");
      Plan p4 = new ProjectPlan(p3, c);
      Scan s4 = p4.open();
      while (s4.next())
         System.out.println(s4.getString("B")); 
      s4.close();
      tx.commit();
   }
}

