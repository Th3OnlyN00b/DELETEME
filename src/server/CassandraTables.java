package server;

import java.util.*;
import java.io.*;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;

import java.io.*;
import java.nio.file.*;
import com.datastax.driver.core.*;

public class CassandraTables implements Replicable {

	/**
	 * CQLSH is path to canssandra cqlsh. To change this path, don't change
	 * the default value below but instead use the system property
	 * -Dcqlsh=/path/to/cqlsh as an argument to gpServer.sh.
	 *
	 */
	private final static String CQLSH = System.getProperty("cqlsh") != null ? System.getProperty("cqlsh") : "/home/ubuntu/apache-cassandra-3.11.1/";
	/**
     * The default keyspace name, as specified by the project docs
     */
    private final static String DEFAULT_KEYSPACE = "demo";
    /**
     * The default table name, as specified by the project docs
     */
    private final static String DEFAULT_TABLE = "CassandraTables0";
    /**
     * This will be the name of the file we dump to
     */
    private final static String DEFAULT_DATA_FILE = "data.csv";
    /**
     * The default tablename to use
     */
    private final static String tableName = DEFAULT_KEYSPACE+"."+DEFAULT_TABLE;

	/**
	 * TODO: implement this checkpoint method
	 */
	@Override
	public String checkpoint(String name) {
        while(true){
            try{
                Process proc = Runtime.getRuntime().exec(new String[]{CQLSH, "-e", "COPY "+tableName+" TO '"+DEFAULT_DATA_FILE+"';"});
                proc.waitFor();
                return DEFAULT_DATA_FILE;
            } catch (IOException e){
                //e.printStackTrace();
            } catch (InterruptedException e){
                //e.printStackTrace();
            }
        }
	}

	/**
	 * TODO: implement this execute method
	 */
	@Override
	public boolean execute(Request request) {
        if(true) return true;	
        // execute request here
        System.out.println("HERE'S THE REQUEST");
        System.out.println(request.toString());
        

        Cluster cluster;
        Session session;

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect(DEFAULT_KEYSPACE);

        session.execute(request.toString());
        cluster.close();
        return true;
	}
	
	/**
	 * no need to implement this method, implement the execute above
	 */
	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) {
		// execute request without replying back to client

		// identical to above unless app manages its own messaging
		return this.execute(request);
	}

	/**
	 * TODO: implement this restore method
	 */
	@Override
	public boolean restore(String name, String state){
        try{ 
            // Restore data from the file
            Process proc = Runtime.getRuntime().exec(new String[]{CQLSH, "-e", "COPY "+tableName+" from '"+DEFAULT_DATA_FILE+"';"});
            proc.waitFor();
            return true;
        } catch (IOException e){
            //e.printStackTrace();
        } catch (InterruptedException e){
            //e.printStackTrace();
        }
        return false;
	}

	/**
	 * No need to implement unless you want to implement your own
	 * packet types (optional).
	 */
	@Override
	public Request getRequest(String req) throws RequestParseException {
		return null;
	}

	/**
	 * No need to implement unless you want to implement your own
	 * packet types (optional).
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return null;
	}

}
