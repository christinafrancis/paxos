package paxos;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

// Handlers are the servers in the distributed system

public class Handler implements Runnable{
	
	public boolean isRequest;
	public boolean isLeader;
	public int serverTag;
	public int portNum;
	public int startPropNum;
	
	DatagramSocket dSocket;
	
	//----------------Used by Acceptor
	AcceptorPromise pr;
	Map<Integer,Boolean > isFirstReqForRound_Phase1; 
	Map<Integer,Boolean > isFirstReqForRound_Phase2; 
	
	//----------------Used by Proposer for each round
	Map<Integer,Integer > countResForRound_Phase1; // counting responses to check for majority in each round
	Map<Integer,Integer > countResForRound_Phase2; 
	Map<Integer,Integer > countACKForRound_Phase1; // counting responses to check for majority in each round
	Map<Integer,Integer > countACKForRound_Phase2; 
	Map<Integer,Integer > propNumForRound;
	Map<Integer,Integer > highestPropNumForRound; // highest proposal num responded by acceptors so far  --- used by proposer
	ValueCount vc; // In Phase 2, whichever value/cmd has the highest occurrence from received ACKs
	ValueCount vcPhase1;
	//----------------
	
	Map<Integer, String > valueForRound;
	
	//int round = 0;
	int proposalNum ;
	private Queue<String> commandList;
	
	public RequestResponseMsg rq;
	public RequestResponseMsg rq1;
	public RequestResponseMsg rs;
	
	ProposalMsg msg;
	String cmd;
	
	Handler( boolean isProposer, int startPropNum,  int serverTag ){
		
		this.portNum = ServerConstants.SERVER_PORT[serverTag];
		proposalNum = startPropNum;
		cmd = ServerConstants.COMMAND[serverTag];
		this.startPropNum = startPropNum;
	//--	msg = new ProposalMsg(this.startPropNum,cmd);
		
		if ( isProposer ){
			try {
				Thread.sleep(ServerConstants.SLEEP_INTERVAL[serverTag]);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			isLeader = true;
			
			// Queue up commands and start Phase 1 
			//commandList = new LinkedList<String>();
			//enqueueCommand("Withdraw50");
			//enqueueCommand("Deposit40");
			//enqueueCommand("Withdraw60");
			//enqueueCommand("balanceCheck");
			/* Send command by command to Acceptors
			 *  Create Request and invoke Request.sendTo 
			 *
			Iterator<String> i = commandList.iterator();
			while( i.hasNext() ){
				cmd = i.next();
				propose( cmd );
			}*/
			
			propose(cmd);
		}
		else{
			isLeader = false;
			
		}	
		this.serverTag = serverTag;
		isFirstReqForRound_Phase1 = new HashMap<Integer,Boolean>();
		isFirstReqForRound_Phase2 = new HashMap<Integer,Boolean>();
		countResForRound_Phase1 = new HashMap<Integer,Integer>();
		countResForRound_Phase2 = new HashMap<Integer,Integer>();
		countACKForRound_Phase1 = new HashMap<Integer,Integer>();
		countACKForRound_Phase2 = new HashMap<Integer,Integer>();
		propNumForRound = new HashMap<Integer,Integer>();
		highestPropNumForRound = new HashMap<Integer,Integer>();
		valueForRound = new HashMap<Integer, String>();
		vc = new ValueCount();
		vcPhase1 = new ValueCount();
		pr = new AcceptorPromise( null, 0 , 0); // AcceptorPromise(cmd, prop_num, round)  -- in Phase 1, command cannot be accepted
		
		for( int r = 1; r <= PaxosConstants.TOTAL_ROUNDS; r++ ){
			isFirstReqForRound_Phase1.put(r, true);
			isFirstReqForRound_Phase2.put(r, true);
			countResForRound_Phase1.put(r,0); 
			countResForRound_Phase2.put(r,0); 
			countACKForRound_Phase1.put(r,0); 
			countACKForRound_Phase2.put(r,0); 
			propNumForRound.put(r,proposalNum);
			highestPropNumForRound.put(r,proposalNum);
			pr.setHighProposalNum(r, 0);
			pr.setAcceptedProposalValue(r, null);
			
		}
		
	}
	
	// Clients send commands to Proposer. Proposer then adds to Queue.
	private void enqueueCommand( String cmd ){
		commandList.add( cmd );
	}
	@SuppressWarnings("unused")
	private String firstCommand(){
		//String cmd = commandList.peek();
		return "xyz";
	}
	private void dequeueCommand(){
		//commandList.remove();
	}
	
	public void propose(String cmd){
		
		try {
			
			
			Debug.print("Created msg*** *** *** *** now  " + proposalNum + ":" + cmd);
			msg = new ProposalMsg(proposalNum, cmd); // Proposal_msg( num, cmd)
			// Request( phase, round, Proposal_msg, port_num of destination)
			//	for(int i=0; i<30; i++){
					// i represents rounds
					// each round requires it's own Quorum of Acceptors
					
					
					for (int  i : ServerConstants.SERVER_PORT){
									
									if ( i != portNum ){
										//Debug.print(i + "server_num");
										rq = new RequestResponseMsg(PaxosConstants.PHASE_1,1,msg,i);// RequestResponseMsg(int phaseNum, int roundNum,  Proposal_msg prop, int portNum)
										rq.sendTo(InetAddress.getByName("127.0.0.1"));
									}
											
								}
					
				//}
			
			
		
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		
		DatagramPacket dPacket;
		byte [] packetBuff = new byte[4096];
		Debug.print("Inside Handler");
		
		try {
			
			dSocket = new DatagramSocket(null);
			dSocket.setReuseAddress(true);
			dSocket.bind(new InetSocketAddress(portNum));
			dPacket = new DatagramPacket(packetBuff, packetBuff.length);
			
			Debug.print("Inside Handler");
			while (true) {
				byte [] recvBuff;
				dSocket.receive(dPacket);
				recvBuff = dPacket.getData();
				
				InetAddress sourceIp = dPacket.getAddress();
				int sourcePort = dPacket.getPort();
				
				
				//Debug.print(" Server with PortNum :" + portNum + "Got msg (" + recvBuff.length + "bytes) from client " + sourceIp.getHostAddress());
				
				/* De-serialize byte array */
				ByteArrayInputStream byteInputStream = new ByteArrayInputStream(recvBuff);
				ObjectInputStream objInputStream = new ObjectInputStream(byteInputStream);
				
			
				RequestResponseMsg message = (RequestResponseMsg) objInputStream.readObject();
				handleMsg(message, sourceIp, sourcePort);
		
		
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		finally{
			 if (dSocket != null) {
			      dSocket.close();
			    }
			 Debug.print("Inside finally - closing " + this.portNum);
		}
		
		
		
	}
	
	 
	 public void finalize() {
		    if (dSocket != null) {
		      dSocket.close();
		    }
		  }
	public void handleMsg( RequestResponseMsg message, InetAddress sourceIp, int sourcePort){
		
		
		//else
		//	pr.setHighProposalNum(message.round, message.msg.proposalNum );
		
		
		if( message.msg_type == PaxosConstants.REQUEST ){
			//Debug.print("Handling request message: InetAdd-"+sourceIp+ " sourcePort " + sourcePort + " reqMsg - "+message);
			/*
			 * Requests are handled by Acceptors
			 * 1. Check if request is Phase 1: Prepare Request
			 * 	if Phase 1 : Check if proposal number is greater than the proposal number for which it had promised in this round.
			 * 			 The promise is not to accept any other proposals with a lesser number. (Check Log entry of this Acceptor to set AcceptedBefore)
			 * 		if satisfied: Reply ACK with a proposal it had accepted earlier in this round ( if any )
			 * 		if not: Reply NACK
			 * if Phase 2 : Send ACK or NACK based on the promise made in Phase 1. 
			 * 				: Log Accepted proposal for this round. 
			 */
			ProposalMsg replyProp = new ProposalMsg(0,null);
			int acceptedBefore = PaxosConstants.ALREADY_NOT_ACCEPTED;
			int ack = PaxosConstants.NACK;
			int phase = PaxosConstants.PHASE_1;
			
			
			Debug.print("phase: " + message.phase);
			if(message.phase == PaxosConstants.PHASE_1){
				
				
				
				Debug.print("Phase 1: Request for cmd:" +message.msg.value);
				
				if( isFirstReqForRound_Phase1.get(message.round) ){
					pr.setHighProposalNum(message.round, message.msg.proposalNum); // AcceptorPromise(cmd, prop_num, round)  -- in Phase 1, command cannot be accepted
					isFirstReqForRound_Phase1.put(message.round, false);
				}
				
				if(message.msg.proposalNum < pr.getProposalNumber(message.round)){
					Debug.print("Phase 1:NACK, already promised to "+ pr.getProposalNumber(message.round)+ " prop num in message :" + message.msg.proposalNum);
					
					ack = PaxosConstants.NACK;
					
				}
				else{
					ack = PaxosConstants.ACK;
					Debug.print("Phase 1:ACK , already promised to "+ pr.getProposalNumber(message.round)+ " prop num in message :" + message.msg.proposalNum);
					pr.setHighProposalNum(message.round, message.msg.proposalNum );
					// Create a Response message with command that is Accepted already.
					
					replyProp.proposalNum = pr.getProposalNumber(message.round);
					
					
					
				}
				
			}
			else{
				Debug.print("Phase 2: Request for cmd:" +message.msg.value);
				
				phase = PaxosConstants.PHASE_2;// 
				if( isFirstReqForRound_Phase2.get(message.round) ){
					int temp ;
					temp = pr.getProposalNumber(message.round);
					if(temp <= message.msg.proposalNum){
						pr.setAcceptedProposalValue(message.round, message.msg.value);
						Debug.print("Value set%%%%%%%%%%%%%%%%%" + pr.getProposalValue(message.round));
						acceptedBefore = PaxosConstants.ALREADY_ACCEPTED;
						valueForRound.put(message.round, message.msg.value);
						System.out.println("Server Tag -"+serverTag+ " :: Round -"+message.round+" :: valueFoRound -" + valueForRound.get(message.round));
						isFirstReqForRound_Phase2.put(message.round, false);
					}
					pr.setHighProposalNum(message.round,Math.max(message.msg.proposalNum, temp)); // -- in Phase 2, set message.value once accepted
					
				}
				
				if(message.msg.proposalNum < pr.getProposalNumber(message.round)){
					Debug.print("NACK , already promised to "+ pr.getProposalNumber(message.round)+ " prop num in message :" + message.msg.proposalNum);
					
					ack = PaxosConstants.NACK;
					
				}
				else{
					Debug.print("ACK , already promised to "+ pr.getProposalNumber(message.round)+ " prop num in message :" + message.msg.proposalNum);
					
					ack = PaxosConstants.ACK;
					
	
				}
			}
			if(pr.getProposalValue(message.round) != null){
				acceptedBefore = PaxosConstants.ALREADY_ACCEPTED;
			}
			else{
				acceptedBefore = PaxosConstants.ALREADY_NOT_ACCEPTED;
			}
			//----
			
			replyProp.proposalNum =pr.getProposalNumber(message.round);
			replyProp.value = pr.getProposalValue(message.round);
			

			rs = new RequestResponseMsg(phase, message.round, ack, acceptedBefore, replyProp, 0 );// RequestResponseMsg(int phaseNum, int roundNum, int ackVal, int accepted, ProposalMsg prop, int portNum)
			rs.msg_type = PaxosConstants.RESPONSE;
			sendMsgToOtherServers(rs);
		}
		else if (isLeader == true){
			//Debug.print("Handling response message: InetAdd-"+sourceIp+ " sourcePort " + sourcePort + " resMsg - "+message);
			/*
			 * Responses are handled by Proposers
			 * 1. Check if response is Phase 1: Response to Prepare Request (or) Phase 2: Accept Request
			 * 	if Phase 1 : 
			 *	    if ACK: Check for ALREADY_ACCEPTED flag - 
			 *		 	if ALREADY_ACCEPTED : Look into Proposal got from that Acceptor.
			 *		 	if ALREADY_NOT_ACCEPTED : There won't be any proposal in the message. 
			 *		 	(Wait till it attains majority and initiate Phase 2 request with the proposal having highest number so far from the obtained set)
			 *		if NACK: Continue Phase 1 request after raising the value of ProposalNum; 
			 * if Phase 2 : if ACK: learn the proposal and log it.
			 * 				if NACK: carry out Phase 1 request again after raising the value of the ProposalNum.
			 * 
			 * For every ACK in Phase 2, increment round
			 * 	Map<Integer,Integer > countResForRound_Phase1; // counting responses to check for majority in each round
				Map<Integer,Integer > countResForRound_Phase2; 
				Map<Integer,Integer > propNumForRound;
			 */
			// See propose() - which has ProposalMsg . It is 0th round proposal for each command
			msg = new ProposalMsg(this.startPropNum,cmd);
			int phase = PaxosConstants.PHASE_1;
			boolean propose = false;
			int temp;
			boolean valChange = false;
			String tempStr = ServerConstants.COMMAND[this.serverTag];
			
			if(message.phase ==  PaxosConstants.PHASE_1){
				
				temp = countResForRound_Phase1.get(message.round) + 1;
				countResForRound_Phase1.put(message.round, temp );// old value gets replaced for the key
				
				
				Debug.print("Received PHASE 1 RESPONSE ");
				
				if(message.ackVal == PaxosConstants.ACK){
					Debug.print("Received PHASE 1 RESPONSE :Reached ACK cmd accapeted berore in promise:" + message.msg.value);
					temp = countACKForRound_Phase1.get(message.round) + 1;
					countACKForRound_Phase1.put(message.round, temp );// old value gets replaced for the key
					
					
					phase = PaxosConstants.PHASE_2;// Next send Phase 2 req
					
					if(message.acceptedBefore == PaxosConstants.ALREADY_ACCEPTED){
						Debug.print("Received PHASE 1 RESPONSE  :Reached ALREAADYY_ACCEPTED ");
						  // Creating a new message to send Phase 2 request for this round
						
							vcPhase1.addCount(message.round, message.msg.value);
							Debug.print("Value changed from "+msg.value + " to " + message.msg.value);
						
							
							if(msg.proposalNum < highestPropNumForRound.get(message.round)){
								setHighestPropNumForRound(message.round,message.msg.proposalNum );
								msg.proposalNum = highestPropNumForRound.get(message.round);
							}
						

					}
					else{
						vcPhase1.addCount(message.round, msg.value);
						if(msg.proposalNum < highestPropNumForRound.get(message.round)){
							setHighestPropNumForRound(message.round,message.msg.proposalNum );
							msg.proposalNum = highestPropNumForRound.get(message.round);
					}
					
				}// In else,    Send the same message.msg in Phase 2
						
				
			   }
				
				if(countResForRound_Phase1.get(message.round) >= PaxosConstants.QUORUM_MAJORITY){
					if(countACKForRound_Phase1.get(message.round) >= PaxosConstants.QUORUM_MAJORITY){
						Debug.print("Declare Majority ACK in Phase 1 for cmd" + msg.value);
						phase = PaxosConstants.PHASE_2;
						msg.value = vcPhase1.getValueWithMaxCount(message.round);
					}
					else{
						phase = PaxosConstants.PHASE_1;
						msg.proposalNum = raiseProposalNum(message.round);
					}
					propose = true;
				}
				else{
					propose = false;
				}
				
		}
			else{
				
				Debug.print("Received PHASE 2 RESPONSE ");
				
				
				temp = countResForRound_Phase2.get(message.round) + 1;
				countResForRound_Phase2.put(message.round, temp );// old value gets replaced for the key
				
				if(message.ackVal == PaxosConstants.ACK){
					
					
					if(message.acceptedBefore == PaxosConstants.ALREADY_ACCEPTED){
						
						 temp = countACKForRound_Phase2.get(message.round) + 1;
						 countACKForRound_Phase2.put(message.round, temp );// old value gets replaced for the key
							
						Debug.print("Received PHASE 2 RESPONSE  :Reached ALREAADYY_ACCEPTED ");
						  // Creating a new message to send Phase 2 request for this round
						
							vc.addCount(message.round, message.msg.value);
						
					}
					else{// NACK phase 2
						phase = PaxosConstants.PHASE_1;
						msg.proposalNum = raiseProposalNum(message.round);
						
					}
					
					// Add this to log
				}
				else{// NACK phase 2
					phase = PaxosConstants.PHASE_1;
					msg.proposalNum = raiseProposalNum(message.round);
					
				}
				if(countResForRound_Phase2.get(message.round) >= PaxosConstants.QUORUM_MAJORITY){
					if(countACKForRound_Phase2.get(message.round) >= PaxosConstants.QUORUM_MAJORITY){
						//Must log here
						String finalValue;
						if( vc.getValueWithMaxCount(message.round) == null){
							System.out.println("ERRORRRRRRRRRRRRRRRRRRRRRRRRR");
							finalValue = msg.value;
						}
						else
							finalValue = vc.getValueWithMaxCount(message.round);
						Debug.print("Declare Majority in Phase 2  for cmd" + finalValue);
						valueForRound.put(message.round, finalValue);
						System.out.println("Server Tag -"+serverTag+ " :: Round -"+message.round+" :: valueFoRound -" + valueForRound.get(message.round));
						propose = false;
						if(!tempStr.contentEquals(valueForRound.get(message.round)) )
							valChange = true;
						else
							valChange = false;
						
					}
					else if(!valueForRound.containsKey(message.round))
						propose = true;
				}
			}
			
			
			if(propose && !valueForRound.containsKey(message.round) && !valueForRound.containsValue(tempStr) ){
				
				rq1 = new RequestResponseMsg(phase, message.round, msg, 0 );// RequestResponseMsg(int phaseNum, int roundNum,  ProposalMsg prop, int portNum)
				rq1.msg_type = PaxosConstants.REQUEST;
				sendMsgToOtherServers(rq1);
				
			}
			else if(valChange){
				Debug.print("At Server :"+serverTag+"Change in value occured**************************"+msg.value+" to "+valueForRound.get(message.round));
				
				if(!valueForRound.containsValue(tempStr)){
					msg.value = tempStr;
					msg.proposalNum = propNumForRound.get(message.round + 1);
					rq1 = new RequestResponseMsg(PaxosConstants.PHASE_1, message.round+1, msg, 0 );// RequestResponseMsg(int phaseNum, int roundNum,  ProposalMsg prop, int portNum)
					rq1.msg_type = PaxosConstants.REQUEST;
					sendMsgToOtherServers(rq1);
				}

			}
			
		}
		
		
	}
	

	
	
	private void setHighestPropNumForRound(int round, int num){
		if(highestPropNumForRound.get(round) < num){
			highestPropNumForRound.put(round, num);
			
		}
	}
	
	private int raiseProposalNum( int round ){
		
		proposalNum = propNumForRound.get(round) + Paxos.servers.size();
		propNumForRound.put(round, proposalNum); // New proposal numbers will be raised in powers of server_tag
		return proposalNum;
	}
	
	private void sendMsgToOtherServers( RequestResponseMsg rs){
		for (int  i : ServerConstants.SERVER_PORT){
			
			if ( i != portNum ){
				//Debug.print(i + "server_num");
	
				try {
					
					rs.portNum = i;
					rs.sendTo(InetAddress.getByName("127.0.0.1"));
					
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
					
		}
	}

}

