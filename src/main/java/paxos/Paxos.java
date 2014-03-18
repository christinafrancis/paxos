package paxos;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;




public class Paxos {

	/**
	 * @param args
	 */
	static ArrayList<Thread> servers;

	public static void main(String[] args) {
		
		 
		
		 servers = new ArrayList<Thread>();
		
		 //Server strtPropNum are set as unique prime numbers to generate different proposal numbers in each server.
		// Example server startPropoNum-3 can generate {3,9,81,243,..}; server 5 -{5,25,125,625,..} and so on..
		Thread c2 = ( new Thread( new Handler( true,  1, 1) ));//Handler( boolean isProposer, int startPropNum,  int serverTag )
		servers.add ( new Thread( new Handler( false,  2, 2) ));
		servers.add( new Thread( new Handler( false,  3, 3) ));
		servers.add( new Thread( new Handler( true,  4, 4) ));
		//Thread c1 =( new Thread( new Handler( true,  0, 0) ));
		
		
		
		 //ExecutorService threadPool = Executors.newFixedThreadPool(5);
		
		 //Debug.print("servers.size = " + servers.size() );
		 
		 for( int i = 0; i< servers.size(); i++){
			 servers.get(i).start();
		 }
		 //servers.get(0).start();
		 
		
		 Thread c1 = ( new Thread( new Handler( true,  0, 0) ));
			
			c1.start();
			c2.start();
			
		 
	}
	
}
