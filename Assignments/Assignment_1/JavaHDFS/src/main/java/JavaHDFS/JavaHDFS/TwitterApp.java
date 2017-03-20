package JavaHDFS.JavaHDFS;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.RequestToken;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;



public class TwitterApp 
{
	final static Logger log = Logger.getLogger(TwitterApp.class);
	
    public static void main( String[] args ) throws TwitterException, IOException
    {
    	Twitter twitter = TwitterFactory.getSingleton();
        twitter.setOAuthConsumer("Xh2Okw8Ws1PngZA014EBY5i5R", "iPNsyr05qEEzzV2jaX4OOXVDPVn9FMWVMFLmOLiz5eJxUbI6QR");
        RequestToken reqToken = twitter.getOAuthRequestToken();
        //AccessToken accessToken = null;
        BufferedReader bufr = new BufferedReader(new InputStreamReader(System.in));
        
        	log.info("Click or Open the below URL ");
        	//System.out.println("Click or Open the below URL ");
        	
        	log.info(reqToken.getAuthorizationURL());
        	System.out.println(reqToken.getAuthorizationURL());
        	
        	log.info("Authorize axp161730 to use your account? Click on Authorize App");
        	//System.out.println("Authorize axp161730 to use your account? Click on Authorize App");
        	log.info("Copy the PIN and paste below. Press ENTER ");
        	//System.out.println("Copy the PIN and paste below. Press ENTER ");

        	String[] query_str={ 	"Trump since:2016-01-01",
        							"Trump since:2016-02-01",
        							"Trump since:2016-03-01",
        							"Trump since:2016-04-01",
        							"Trump since:2016-05-01",
        							"Trump since:2016-06-01"};
        	
        	String auth_pin = bufr.readLine();
        		
        		if(auth_pin.isEmpty())
        		{
        			System.out.println("Authorization failed");
        			log.info("Authorization failed");
        		}
        		else
        		{
        			try
        			{
        			twitter.getOAuthAccessToken(reqToken,auth_pin);
        			
        			 bufr.close();
        		    	Twitter twit = TwitterFactory.getSingleton();
        		    	for(int i=0;i<6;i++)
        		    	{
        		    		
        		    	
        		        Query query = new Query(query_str[i]);
        		        
        		        query.count(100);
        		        QueryResult result = twit.search(query);
        		        System.out.println("Downloading tweets");
            			log.info("Downloading tweets");
            			int j=i+1;
            			System.out.println("Saving in file :Twitter_file_"+j+".txt");
            			log.info("Saving in file :Twitter_file_"+j+".txt");
            			
            			String file="Twitter_file_"+j+".txt";
            			
        		        FileWriter fw = new FileWriter(file);
        		        BufferedWriter bufw = new BufferedWriter(fw);
        		        
        		        for (twitter4j.Status status : result.getTweets()) {
        		        	
        		        	bufw.write(status.getText());
        		        }
        		        bufw.close();
        		        
        		        
        		        String files[] = {file,"/user/axp161730/assignment_1/part2/"+file};
        		        FileCopyWithProgress_copy c = new FileCopyWithProgress_copy();
    					try {
							c.copy(files);
							log.info("File :Twitter_file_"+j+".txt copied to HDFS");
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
        		        System.out.println("Tweets downloaded");
        		    	}
        		} catch (TwitterException e) 
        		{
                    if (401 == e.getStatusCode()) 
                    {
                        System.out.println("Unable to get the access token : Wrong Pin");
                    } else 
                    {
                        e.printStackTrace();
                    }
        		}
        		}
        		
        	
    }
}
