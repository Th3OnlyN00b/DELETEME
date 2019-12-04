package client;

import java.util.*;
import java.io.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.umass.cs.utils.Util;
import org.json.JSONException;

import edu.umass.cs.gigapaxos.PaxosClientAsync;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.utils.Config;

/**
 * Use PaxosClientAsync or ReconfigurableClientAsync as in the tutorial
 * examples.
 */
public class CassandraTablesClient {

	/**
	 * default keyspace
	 */
	public final static String DEFAULT_KEYSPACE = "demo";
    private static volatile int count = 0;
	
	public static void main(String[] args) throws Exception{
		PaxosClientAsync client = new PaxosClientAsync();
        InetSocketAddress isa = (args.length > 0) ? Util.getInetSocketAddressFromString(args[0]) : null;
        System.out.println("I begin");
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(System.in));
        
        Object lock = new Object();
        int commandNum = 0;
        String cmd;
        while((cmd = stdInput.readLine()) != null){
            commandNum++;
            System.out.println("Sending request " + commandNum);
            if(isa == null){
                client.sendRequest(PaxosConfig.getDefaultServiceName(), cmd, new RequestCallback() {
                    @Override
                    public void handleResponse(Request response) {
                        System.out.println("Waiting for lock...");
                        synchronized(lock){
                            System.out.println("Got lock!");
                            count++;
                        }
                    }
                });
            } else {
                client.sendRequest(PaxosConfig.getDefaultServiceName(), cmd, isa, new RequestCallback() {
                    @Override
                    public void handleResponse(Request response) {
                        System.out.println("Waiting for lock...");
                        synchronized(lock){
                            System.out.println("Got lock!");
                            count++;
                        }
                    }
                });
            }
        }
        Thread.sleep(5000);
        if(count < commandNum){
            System.out.println("Failed to excecute all commands due to timeout.");
            System.exit(1);
        }
        System.exit(0);
	}

}
