package paxos;

import java.util.HashMap;
import java.util.Map;



public class ValueCount {
	
	//Map of Maps
	
	Map<Integer, Map<String,Integer>> roundCount;
	Map<String, Integer> countValue;
	
	public ValueCount(){
		roundCount = new HashMap<Integer, Map<String,Integer>>();
		countValue = new HashMap<String, Integer>();
		
	}
	
	public void addCount( int round, String val){
		if(!roundCount.containsKey(round)){
			
			countValue = new HashMap<String, Integer>();
			countValue.put(val, 1);
			roundCount.put(round, countValue);
		}
		else{
			if (roundCount.get(round).containsKey(val)) {
				int temp;
				temp = (roundCount.get(round)).get(val);
				temp++;
				countValue.put(val, temp);
				roundCount.put(round, countValue);
			}
			else{
				roundCount.get(round).put(val, 1);
			}
		}
	}
	
	public String getValueWithMaxCount(int round){
		String tempVal = null;
		int tempMax = 0;
		int temp;
		if(roundCount.containsKey(round)){
			for ( Map.Entry<String, Integer> c: roundCount.get(round).entrySet()){
				temp = c.getValue();
				if(temp >= tempMax){
					// This has highest frequency till now
					tempMax = temp;
					tempVal = c.getKey();
				}
			}
		}
		
		
		
		
		return tempVal;
	}
}
