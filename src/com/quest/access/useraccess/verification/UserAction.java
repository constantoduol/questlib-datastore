package com.quest.access.useraccess.verification;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.quest.access.common.UniqueRandom;
import com.quest.access.common.datastore.Datastore;
import com.quest.access.control.Server;
import com.quest.servlets.ClientWorker;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpSession;

/**
 *
 * @author constant oduol
 * @version 1.0(23/6/12)
 */

/**
 * This class tries to store information concerning actions of users in the system
 * this is done using a verification process where users with the relevant privileges
 * or relevant authority can carry out actions in the system.
 * @author Conny
 */
  public class UserAction implements Action{  
      
      private String actionID;
      private String description;
      private HttpSession clientSession;
    
      /**
       * Constructs a user action object that describes what a user has done
       * the action is not saved until the instance method saveAction is called
       */
      
      public UserAction(ClientWorker worker, String description){
    	  UniqueRandom ur = new UniqueRandom(50);
          this.actionID = ur.nextMixedRandom();
          this.description = description;   
          this.clientSession = worker.getSession();
      } 
  
      
      /**
       * 
       * @return a string representing the id of this action
       */
    @Override
      public String getActionID(){
          return this.actionID;
      }
      
      /**
       * this method is called to commit an action as performed by a specific user
       */
    @Override
      public void saveAction(){
          String userName=(String) this.clientSession.getAttribute("username");
          String userID=(String) this.clientSession.getAttribute("userid");
          Entity en = new Entity("USER_ACTIONS");  
          en.setProperty("ACTION_ID",this.actionID);
          en.setProperty("USER_ID",userID);
          en.setProperty("USER_NAME",userName);
          en.setProperty("ACTION_TIME", System.currentTimeMillis());
          en.setProperty("ACTION_DESCRIPTION",this.description);
          Datastore.insert(en);
      }
      
      /**
       * this method is called to get details concerning a specific action is string
       */
      public static HashMap<String,Object> getActionDetails(String actionId){
    	  Iterable<Entity> entities = Datastore.getMultipleEntities("USER_ACTIONS", "ACTION_ID", actionId,FilterOperator.EQUAL);
    	  HashMap<String,Object> details = new HashMap<>();
    	  for(Entity en : entities){
              details.put("USER_ID",en.getProperty("USER_ID"));
              details.put("ACTION_TIME",en.getProperty("ACTION_TIME"));
              details.put("USER_NAME",en.getProperty("USER_NAME"));
              details.put("ACTION_DESCRIPTION",en.getProperty("ACTION_DESCRIPTION")); 
    	  }
          return details;
      }
      
}
