package paxos;

import java.util.HashMap;
import java.util.Map;

// Used by Acceptors to get the highest numbered proposal to which promise was made. 

//For the current round, it provides the highest <value, proposal Num>
// Ballot and Round are one and the same.

//Each Acceptor maintains its own log - contains the round number as key and the proposal accepted as value
//Replace entry occurs when a higher numbered proposal with the same value is accepted in phase-2 

public class AcceptorPromise {
	
	Map<Integer,HighProposal> roundHigh; // In Phase1, Highest Proposal number obtained so far for each round.
	HighProposal hp;
	
	public AcceptorPromise(String cmd, int val, int round){ // Constructor called by Acceptor, whenever it receives a Proposal for the first time in a round.
		
		hp = new HighProposal(0,null);
		roundHigh = new HashMap<Integer,HighProposal>();
		roundHigh.put(round, hp);
		/*for( int r = 0; r < PaxosConstants.TOTAL_ROUNDS; r++ ){
			hp = new HighProposal(val, cmd);
			roundHigh = new HashMap<Integer,HighProposal>();
			roundHigh.put(round, hp);
		}*/
	}
	
	public void setHighProposalNum(int round, int num){
		
		if(roundHigh.containsKey(round)){
			replaceEntry(round,num);
		}
		else{
			hp = new HighProposal(num, null);
			roundHigh.put(round, hp);
		}
			
	}
	private void replaceEntry(int round, int propNum){
		
		if (roundHigh.get(round).num < propNum ){
			roundHigh.get(round).num = propNum;
			roundHigh.put(round, roundHigh.get(round));
		}
	}
	
	public String getProposalValue(int round){
		
		return roundHigh.get(round).cmd;
	}
	public int getProposalNumber(int round){
		
		return roundHigh.get(round).num;	
	}
	
	public void setAcceptedProposalValue(int round, String val){
		
		if(roundHigh.containsKey(round)){
			replaceEntry(round,val);
		}
		else{
			hp = new HighProposal(0, null);
			roundHigh.put(round, hp);
		}
	}
    private void replaceEntry(int round, String val){
		
		
			roundHigh.get(round).cmd = val;
			roundHigh.put(round, roundHigh.get(round));
		
	}


	private class HighProposal{ // Has the highest proposal number obtained so far and the cmd value of the first proposal received.
		
		int num;
		String cmd;
		
		public HighProposal(int highestProposalNum, String val){
			num = highestProposalNum;
			cmd = val;
		}
	}
}