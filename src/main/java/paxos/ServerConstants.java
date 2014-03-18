package paxos;

public final class ServerConstants {
	
	// PORT number for each server.  UDP communication.
	
	public static final int[] SERVER_PORT = {26551,26552,26553,26554,26555};
	
	public static final int[] SLEEP_INTERVAL = {1000,2000,4000,6000,8000};
	
	public static final String[] COMMAND = {"Withdraw50","Deposit 45","Balance","Deposit30","Withdraw70"};
	

}
