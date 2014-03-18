package paxos;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class RequestResponseMsg implements Serializable{

	private static final long serialVersionUID = 1L;
	//public static final int RECEIVER_PORT = 9094;
	
/*
 * Exclusive values for Msg of type Response : ack and acceptedBefore
 */
	int ackVal;
	// Below member values are set only during Phase 1 Acceptance if any Proposal has already been accepted for the round
	int acceptedBefore; // Already Accepted some proposal in the requested Round : [Proposal_msg is returned]
/*
 * 
 */
	ProposalMsg msg;
				// During Phase 1, only msg.propNum is set; msg.value is ignored in phase 1 by Acceptors
	int round;
	int phase;	// Phase can be 1 or 2
	int msg_type; // Request / Response
	int portNum; //  port of source that sends request
	
	public RequestResponseMsg(int phaseNum, int roundNum,  ProposalMsg prop, int portNum){

		msg_type = PaxosConstants.REQUEST;
		
		msg = new ProposalMsg(prop.proposalNum, prop.value);
		round = roundNum;
		phase = phaseNum;
		Debug.print("sending request at phase"+phaseNum+", with prop num:"+prop.proposalNum);
		this.portNum = portNum; 
	}
	public RequestResponseMsg(int phaseNum, int roundNum, int val, int accepted, ProposalMsg prop, int portNum){
		
		msg_type = PaxosConstants.RESPONSE;
		
		ackVal = val;
		acceptedBefore = accepted;
		msg = new ProposalMsg(prop.proposalNum, prop.value);
		round = roundNum;
		phase = phaseNum;
		this.portNum = portNum;
		
		Debug.print("sending response at phase:"+phaseNum + ", with prop.val:" + prop.value);
	} 
	
	public void sendTo(InetAddress ip) {

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream out;
		
		Debug.print("Sending req/resMsg: Source port -  " + portNum);
		
		try {

			/* Serialize object into sendBytes */

			out = new ObjectOutputStream(byteArrayOutputStream);
			out.writeObject(this);
			byte [] sendBytes = byteArrayOutputStream.toByteArray();

			/* Send datagram */
			DatagramSocket dSocket = new DatagramSocket();
			DatagramPacket sendPacket = new DatagramPacket(sendBytes, sendBytes.length, ip, portNum);
			dSocket.send(sendPacket);
			dSocket.close();

		} 

		catch (Exception e) {
			e.printStackTrace();
		}	

	}
}
