package simpledb.server;

import java.rmi.registry.*;
import simpledb.jdbc.network.*;

public class StartServer {

   static final String NAME = System.getProperty("simpledb.server.name", "simpledb");
   static final int PORT = Integer.getInteger("simpledb.server.port", 1099);

   public static void main(String args[]) throws Exception {
      String dirname = "data";

      // Configure and initialize the database
      if (args.length > 0) {
         dirname = args[0];
      }
      SimpleDB db = new SimpleDB(dirname);
      
      // Create a registry specific for the server on the default port
      Registry reg = LocateRegistry.createRegistry(PORT);
      // And post the server entry in it
      RemoteDriver d = new RemoteDriverImpl(db);
      reg.rebind(NAME, d);

      db.info("Database server listen on 'rmi://localhost:%d/%s'", PORT, NAME);
   }

}
