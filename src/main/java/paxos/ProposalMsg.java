package paxos;

import java.io.Serializable;



public class ProposalMsg implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	int proposalNum;
	String value; // value represents the command
	
	public ProposalMsg(int num, String cmd){
		proposalNum = num;
		value = cmd;
	}

}
