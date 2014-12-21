package edu.sjsu.cmpe273.ClientCRDT;

public class Client {

    public static void main(String[] args) throws Exception 
    {
        System.out.println("Starting Cache Client...");
        
        ClientCRDT crdtClient = new ClientCRDT();
        boolean requestStatus = crdtClient.put(1, "a");
        if (requestStatus) 
        {
        	System.out.println("1. Write Finished, Thread sleeping for 30secs!");
        	Thread.sleep(30000);
        	requestStatus = crdtClient.put(1, "b");
        	if (requestStatus) 
        	{
        		System.out.println("2. Write Finished, Thread sleeping for 30secs!");
            	Thread.sleep(30000);
            	String value = crdtClient.get(1);
            	System.out.println("GET value is :"+value);
        	} 
        	else 
        	{
            	System.out.println("Second write failed...");
        	}
        } 
        else 
        {
        	System.out.println("First write failed...");
        }	
     
        System.out.println("Existing Cache Client...");  
    }
}
