package paxos;

public class PaxosConstants {
	
	public static final int PHASE_1 = 1;
	
	public static final int PHASE_2 = 2;
	
	public static final int ACK = 3;
	
	public static final int NACK = 4;
	
	public static final int ALREADY_ACCEPTED = 5;
	
	public static final int ALREADY_NOT_ACCEPTED = 6;
	
	public static final int MSG_FAILURE = 7;
	
	public static final int PAXOS_PREPARE = 8;

	public static final int PAXOS_ACK = 9;

	public static final int PAXOS_ACCEPT = 10;
	
	public static final int REQUEST = 11;
	
	public static final int RESPONSE = 12;
	
	public static final int TOTAL_ROUNDS = 15;
	
	public static final int QUORUM_MAJORITY = 3;
}
