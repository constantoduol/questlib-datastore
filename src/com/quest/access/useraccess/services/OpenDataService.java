/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.quest.access.useraccess.services;

import java.io.IOException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletConfig;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.quest.access.common.UniqueRandom;
import com.quest.access.common.io;
import com.quest.access.common.datastore.Datastore;
import com.quest.access.control.Server;
import com.quest.access.useraccess.NonExistentUserException;
import com.quest.access.useraccess.Serviceable;
import com.quest.access.useraccess.User;
import com.quest.access.useraccess.services.annotations.Endpoint;
import com.quest.access.useraccess.services.annotations.WebService;
import com.quest.access.useraccess.verification.SystemAction;
import com.quest.access.useraccess.verification.UserAction;
import com.quest.servlets.ClientWorker;
import java.util.Iterator;

/**
 *
 * @author Connie
 */
@WebService(name = "open_data_service", level = 10, privileged = "no")
public class OpenDataService implements Serviceable {

    private static final String SERVER_ENDPOINT = "https://quest-uza.appspot.com/server";

    private static final String NEXT_URL = "http://uza.questpico.com";

    private static final long USAGE_THRESHOLD = 50; //a business with more than 50 weighted seconds a month is charged
    
    private static final String[] BILL_TIERS = new String[]{"51-150","151-300","301-450","451-600","601-900","901-1200",
                                                            "1201-1800","1801-3000","3001-4200","4201-6000","6001-12000"};
    
    private static final String[] BILL_PRICES = new String[]{"500","1000","1500","2000","3000","4000","6000","8000","10000","20000","40000"};
    
    private static final String[] BILL_PRICES_DOLLAR = new String[]{"5","10","15","20","30","40","60","80","100","200","400"};
    
    private static final String[] BILL_TIER_NAMES = new String[]{"TIER1","TIER2","TIER3","TIER4","TIER5","TIER6","TIER7","TIER8","TIER9","TIER10","TIER11"};

    

    @Endpoint(name = "fetch_settings",cacheModifiers = {"open_data_service_save_settings"})
    public void fetchSettings(Server serv, ClientWorker worker) {
        JSONObject request = worker.getRequestData();
        String busId = request.optString("business_id");
        Filter filter1 = new FilterPredicate("BUSINESS_ID", FilterOperator.EQUAL, busId);
        JSONObject data = Datastore.entityToJSON(Datastore.getMultipleEntities("CONF_DATA", filter1));
        worker.setResponseData(data);
        serv.messageToClient(worker);
    }


    @Endpoint(name = "business_info")
    public JSONObject getBusinessInfo(Server serv, ClientWorker worker) throws JSONException {
        //we need to get the business id
        JSONObject request = worker.getRequestData();
        String email = request.optString("username");
        Filter filter1 = new FilterPredicate("USER_NAME", FilterOperator.EQUAL, email);
        JSONObject data = Datastore.entityToJSON(Datastore.twoWayJoin(
                new String[]{"BUSINESS_USERS", "BUSINESS_DATA"},
                new String[]{"BUSINESS_ID", "ID"}, null, null,
                new Filter[]{filter1}, new Filter[]{}));
        JSONObject response = new JSONObject();
        response.put("business_ids", data.optJSONArray("BUSINESS_ID"));
        response.put("business_names", data.optJSONArray("BUSINESS_NAME"));
        response.put("business_types", data.optJSONArray("BUSINESS_TYPE"));
        response.put("business_extra_data", data.optJSONArray("BUSINESS_EXTRA_DATA"));
        response.put("business_owners", data.optJSONArray("BUSINESS_OWNER"));
        worker.setResponseData(response);
        serv.messageToClient(worker);
        return response;
    }
    


    @Endpoint(name = "business_data")
    public void businessData(Server serv, ClientWorker worker) throws JSONException {
        //we need to get the business id
        JSONObject request = worker.getRequestData();
        String id = request.optString("business_id");
        Filter filter1 = new FilterPredicate("ID", FilterOperator.EQUAL, id);
        JSONObject data = Datastore.entityToJSONArray(Datastore.getSingleEntity("BUSINESS_DATA", filter1));
        worker.setResponseData(data);
        serv.messageToClient(worker);
    }


    private boolean hasPrivilege(String privilege, ClientWorker worker) {
        JSONArray privs = (JSONArray) worker.getSession().getAttribute("privileges");
        if (privs == null) {
            return false;
        }
        return privs.toList().contains(privilege);
    }

    private List<String> listToLowerCase(List<String> list) {
        ArrayList<String> newList = new ArrayList<>();
        for (String list1 : list) {
            String str = list1.toLowerCase();
            newList.add(str);
        }
        return newList;
    }

    @Endpoint(name = "forgot_password")
    public void forgotPassword(Server serv, ClientWorker worker) throws JSONException {
        try {
            JSONObject details = worker.getRequestData();
            String email = details.optString("username");
            String bussName = details.optString("business_name");
            //check locally to see whether its valid
            User user = User.getExistingUser(email);
            worker.setPropagateResponse(false);
            JSONObject buss = getBusinessInfo(serv, worker);
            worker.setPropagateResponse(true);
            if (!listToLowerCase(buss.optJSONArray("business_names").toList()).contains(bussName)) {
                //no business here
                worker.setResponseData(Message.FAIL);
                worker.setReason("The specified business does not exist for specified email address");
                serv.messageToClient(worker);
                return;
            }
            String body = serv.getEmailTemplate("forgot-password");
            String senderEmail = serv.getConfig().getInitParameter("sender-email");
            String[] from = new String[]{senderEmail, "Quest Pico"};
            String[] to = new String[]{email, email};
            body = body.replace("{user_name}", email);
            String pass = new UniqueRandom(6).nextMixedRandom();
            user.setPassWord(pass);
            user.setUserProperty("CHANGE_PASSWORD", "1", true);
            body = body.replace("{pass_word}", pass);
            body = body.replace("{change_link}", NEXT_URL + "/change.html?user_name=" + email + "&pass_word=" + pass);
            serv.sendEmail(from, to, "Password Reset", body);
            worker.setResponseData(Message.SUCCESS);
            serv.messageToClient(worker);
        } catch (NonExistentUserException ex) {
            worker.setResponseData(Message.FAIL);
            worker.setReason("The specified email address does not belong to any account");
            serv.messageToClient(worker);
        }
    }

    @Endpoint(name = "create_account")
    public void createAccount(Server serv, ClientWorker worker) throws Exception {
        JSONObject details = worker.getRequestData();
        String uName = details.optString("name");
        String realName = details.optString("real_name");
        String busId = details.optString("business_id");
        //if busId is empty, this is a business owner trying to create an account
        UserService us = new UserService();
        worker.setPropagateResponse(false);
        User user = us.createUser(serv, worker);
        ServletConfig config = serv.getConfig();
        if (user != null) {

            String senderEmail = config.getInitParameter("sender-email");
            String[] from = new String[]{senderEmail, "Quest Pico"};
            String[] to = new String[]{uName, realName};
            UserAction action = new UserAction(serv, worker, uName);
            String actionId = action.getActionID();
            String templateRequest = config.getInitParameter("template-request");
            JSONTokener tokener = new JSONTokener(templateRequest);
            JSONObject request = (JSONObject) tokener.nextValue();
            JSONObject requestHeader = request.optJSONObject("request_header");
            JSONObject requestBody = request.optJSONObject("request_object");
            requestHeader.put("request_svc", "open_data_service");
            requestHeader.put("request_msg", "activate_account");
            requestBody.put("user_name", uName);
            requestBody.put("action_id", actionId);
            requestBody.put("business_id", busId);
            requestBody.put("next_url", NEXT_URL);
            String link = SERVER_ENDPOINT + "?json=" + URLEncoder.encode(request.toString(), "UTF-8");

            String subject = config.getInitParameter("create-user-email-subject");
            String body;
            if (busId.isEmpty()) {
                //this is a completely new account 
                body = serv.getEmailTemplate("new-user-account");
            } else {
                body = serv.getEmailTemplate("new-user-account-sub");
                String pass = new UniqueRandom(6).nextMixedRandom();
                body = body.replace("{pass_word}", pass);
                user.setPassWord(pass);

                user.setUserProperty("CHANGE_PASSWORD", "1", true); //force a password change
            }
            body = body.replace("{real_name}", realName);
            body = body.replace("{user_name}", uName);
            body = body.replace("{activation_link}", "<a href=" + link + ">Click to Activate</a>");
            //add this user to this business

            serv.sendEmail(from, to, subject, body);

            user.setUserProperty("IS_DISABLED", "1", true);
            //update his real name in the db
            user.setUserProperty("REAL_NAME", realName, true);
            //disable the user
            action.saveAction();
            worker.setResponseData(Message.SUCCESS);
        } else {
            worker.setResponseData(Message.FAIL);
        }
        worker.setPropagateResponse(true);
        serv.messageToClient(worker);

    }

    @Endpoint(name = "activate_account")
    public void activateAccount(Server serv, ClientWorker worker) throws IOException, NonExistentUserException {
        JSONObject details = worker.getRequestData();
        String email = details.optString("user_name");
        String actionId = details.optString("action_id");
        String nextUrl = details.optString("next_url");
        String busId = details.optString("business_id");
        User user = User.getExistingUser(email);
        //first check if the specified user is already activated
        boolean userExists = !user.getUserProperty("USER_NAME").isEmpty();
        if (!userExists) {
            worker.setResponseData(Message.FAIL);
            worker.setReason("User account seems to be invalid");
            serv.messageToClient(worker);
            return;
        }

        boolean userDisabled = user.getUserProperty("IS_DISABLED").equals("1");
        if (!userDisabled) {
            //if the user is not disabled, it means this account has already been activated
            worker.setResponseData(Message.FAIL);
            worker.setReason("User account has already been activated");
            serv.messageToClient(worker);
            return;
        }
        //here we are dealing with a disabled user
        //check that the action id matches what we have
        HashMap<String, Object> actionDetails = UserAction.getActionDetails(actionId);
        String userName = actionDetails.get("ACTION_DESCRIPTION").toString();
        //if userName === email then we are happy
        if (userName.equals(email)) {
            //well this is a valid activation,do something cool
            //send a redirect to the next url
            //add to specified business
            //enable the user
            user.setUserProperty("IS_DISABLED", "0", true);
            String id = new UniqueRandom(20).nextMixedRandom();
            if (!busId.trim().isEmpty()) {
                String[] propNames1 = new String[]{"ID", "USER_NAME", "BUSINESS_ID", "CREATED"};
                Object[] values1 = new Object[]{id, email, busId, System.currentTimeMillis()};
                Datastore.insert("BUSINESS_USERS", "ID", propNames1, values1);
            }
            worker.getResponse().sendRedirect(nextUrl);
        }

    }
    
    @Endpoint(name = "pay_bill_mpesa")
    public void payBillMpesa(Server serv, ClientWorker worker) throws Exception {
        JSONObject requestData = worker.getRequestData();
        String amount = requestData.optString("AMOUNT");
        String phoneNo = requestData.optString("SENDER_PHONE_NO");
        SystemAction action = new SystemAction("PAY MPESA BILL " + phoneNo + " AMOUNT " + amount + "");
        requestData.put("TIMESTAMP", System.currentTimeMillis());
        requestData.put("ACTION_ID", action.getActionID());
        Datastore.insert("BILL", requestData);
        action.saveAction();
        worker.setResponseData("success");
        serv.messageToClient(worker);
        //paid amount, remaining amount
    }



    public static void calculateAccountBalance(String busId) {
        Filter filter = new FilterPredicate("BUSINESS_ID", FilterOperator.EQUAL, busId);
        Iterable<Entity> bills = Datastore.getMultipleEntities("BUSINESS_BILL", filter);
        Float balance = 0f;
        for (Entity bill : bills) {
    		//a bill can be a debit 0 or a credit 1
            //a credit is when a business pays
            //a debit is when we invoice the business

            String type = bill.getProperty("TRAN_TYPE").toString();
            Float amount = Float.parseFloat(bill.getProperty("AMOUNT").toString().replace(",", ""));
            if (type.equals("1")) {
                balance = balance - amount; //the business owes less
            } else if (type.equals("0")) {
                balance = balance + amount; //the business owes more
            }
        }

        Entity en = Datastore.getSingleEntity("BUSINESS_BILL_BALANCE", filter);
        if (en == null) {
            en = new Entity("BUSINESS_BILL_BALANCE");
            en.setProperty("BUSINESS_ID", busId);
            en.setProperty("BALANCE", balance);
            en.setProperty("LAST_CALCULATED", System.currentTimeMillis());
            Datastore.insert(en);
        } else {
            en.setProperty("BALANCE", balance);
            en.setProperty("LAST_CALCULATED", System.currentTimeMillis());
            Datastore.insert(en);
        }
    }


    public static void main(String[] args) {
        String limit = "300-500";
        Integer limitOne = Integer.parseInt(limit.substring(0, limit.indexOf("-")));
        Integer limitTwo = Integer.parseInt(limit.substring(limit.indexOf("-")+1, limit.length()));
        io.out(limitOne);
        io.out(limitTwo);
        
        
    }

    @Endpoint(name = "invoice_accounts")
    public void invoiceAccounts(Server serv, ClientWorker worker) {
        JSONObject requestData = worker.getRequestData();
        String key = requestData.optString("key");
        if (!key.equals("fwgifgy33hfdegegef")) return; //no false triggers
        
        io.out("Invoicing accounts");
    	//the strategy is to go through the users and check if a user is a 
        //business owner. if a user is a business owner send an invoice to 
        //the user
        //we invoice a business that has reached a certain usage threshold 
        //for a month
        //send an email to the business owner
        Iterable<Entity> bdata = Datastore.getAllEntities("BUSINESS_DATA");
        ArrayList billTiers = getBillTiers();
        for (Entity data : bdata) {
            //dont invoice businesses that dont meet the threshhold
            String busId = data.getProperty("ID").toString();
            Filter filter = new FilterPredicate("BUSINESS_ID", FilterOperator.EQUAL, busId);
            Entity stat = Datastore.getSingleEntity("USAGE_STATS", filter);
            JSONObject conf = Datastore.entityToJSON(Datastore.getMultipleEntities("CONF_DATA", filter));
            Double cpuUsage = Double.parseDouble(stat.getProperty("CPU_USAGE").toString());
            //check whether stat has been exceeded
            if (cpuUsage > USAGE_THRESHOLD) { //bill this guy!
                //create a bill entity
                String transId = new UniqueRandom(20).nextMixedRandom();
                String busName = data.getProperty("BUSINESS_NAME").toString();
                Entity en2 = new Entity("BUSINESS_BILL");
                en2.setProperty("BUSINESS_ID", busId);
                en2.setProperty("TRANS_ID", transId);
                en2.setProperty("TIMESTAMP", System.currentTimeMillis());
                
                String currency = conf.optJSONArray("CONF_VALUE").optString(conf.optJSONArray("CONF_KEY").toList().indexOf("billing_currency"));
                Float billAmount = getBillPrice(cpuUsage, billTiers,currency);
                en2.setProperty("AMOUNT",billAmount);
                en2.setProperty("TRAN_TYPE", "0"); //1 for credit 0 for debit
                en2.setProperty("SENDER_SERVICE", "Quest Invoice");
                Datastore.insert(en2);
                
                //reset the usage stats for this business and record them in usage history
                Entity usageHistory = new Entity("USAGE_HISTORY");
                usageHistory.setPropertiesFrom(stat);
                usageHistory.setProperty("CREATED",System.currentTimeMillis());
                Datastore.insert(usageHistory);
                //reset usage stats for this business to zero
                stat.setProperty("CPU_USAGE",0.0);
                stat.setProperty("USAGE_COUNT", 0);
                Datastore.insert(stat);
                
                
                calculateAccountBalance(busId);//calculate new account balance

                String ownerMail = data.getProperty("BUSINESS_OWNER").toString();
                //send an email here
                String senderEmail = serv.getConfig().getInitParameter("sender-email");
                String[] from = new String[]{senderEmail, "Quest Pico"};
                String[] to = new String[]{ownerMail, ownerMail};

                String body = serv.getEmailTemplate("pay-bill");
                body = body.replace("{trans_id}", transId);
                body = body.replace("{business_name}", busName);
                body = body.replace("{amount}", billAmount.toString() + " "+ currency);
                serv.sendEmail(from, to, "Invoice", body);
            }
    		//get the owner email address
            //get the balance of the account

        }
    }
    //10x30 = 300 that is 10 requests per day is the limit beyond which we start charging
    //
    public static ArrayList getBillTiers(){
        Iterable<Entity> tiers = Datastore.getAllEntities("BILL_TIERS", "TIER_NAME",SortDirection.ASCENDING);
        if(tiers == null){
            //the tiers dont exist so add them to the datastore from the code
            for(int x = 0 ; x < BILL_TIER_NAMES.length; x++){
                Datastore.insert("BILL_TIERS", new String[]{"TIER_NAME","TIER_LIMITS","TIER_PRICE_KES","TIER_PRICE_USD"}, 
                                               new String[]{BILL_TIER_NAMES[x],BILL_TIERS[x],BILL_PRICES[x],BILL_PRICES_DOLLAR[x]});
            }
        }
        //these tiers may have even been updated or changed so read new ones from the datastore
        ArrayList allTiers = new ArrayList();
        for (Entity tier : tiers) {
            String name = tier.getProperty("TIER_NAME").toString();
            String limit = tier.getProperty("TIER_LIMITS").toString();//300-500
            Integer limitOne = Integer.parseInt(limit.substring(0, limit.indexOf("-")));
            Integer limitTwo = Integer.parseInt(limit.substring(limit.indexOf("-") + 1, limit.length()));
            Float priceKes = Float.parseFloat(tier.getProperty("TIER_PRICE_KES").toString());
            Float priceUsd = Float.parseFloat(tier.getProperty("TIER_PRICE_USD").toString());
            allTiers.add(new Object[]{name, limitOne, limitTwo, priceKes,priceUsd});
        }
        
        return allTiers;
    }
    
    public static Float getBillPrice(Double usageCount,ArrayList allTiers,String currency){
        for (Object allTier : allTiers) {
            Object[] tier = (Object[]) allTier;
            Integer limitOne = Integer.parseInt(tier[1].toString());
            Integer limitTwo = Integer.parseInt(tier[2].toString());
            Float price = currency.equals("KES") ? Float.parseFloat(tier[3].toString()) : Float.parseFloat(tier[4].toString()) ;
            if(usageCount >= limitOne && usageCount <= limitTwo){
                return price;
            }
        }
        return 0f;
    }

    @Endpoint(name = "logout")
    public void logout(Server serv, ClientWorker worker) {
        JSONObject requestData = worker.getRequestData();
        String userName = requestData.optString("user_name");
        serv.doLogOut(worker, userName);
        worker.setResponseData("success");
        serv.messageToClient(worker);
    }

    @Endpoint(name = "login")
    public void login(Server serv, ClientWorker worker) throws JSONException, UnknownHostException {
        JSONObject requestData = worker.getRequestData();
        String remoteAddr = worker.getRequest().getRemoteAddr();
        requestData.put("clientip", remoteAddr);
        serv.doLogin(worker);
    }

    @Endpoint(name = "changepass")
    public void changePass(Server serv, ClientWorker worker) {
        JSONObject requestData = worker.getRequestData();
        String userName = requestData.optString("user_name");
        String oldPass = requestData.optString("old_password");
        String newPass = requestData.optString("new_password");
        Boolean change = User.changePassword(userName, oldPass, newPass);
        worker.setResponseData(change);
        serv.messageToClient(worker);
    }
    
    

    @Override
    public void service() {

    }

    @Override
    public void onPreExecute(Server serv, ClientWorker worker) {

    }

    @Override
    public void onStart(Server serv) {
     
    }

    
    @Endpoint(name = "migrate_entities")
    public void migrateEntities(Server serv, ClientWorker worker){
        //current PRODUCT_DATA
        //EXTRA COLUMNS PRODUCT_DATA
        //PRODUCT_CATEGORY,PRODUCT_SUB_CATEGORY,PRODUCT_PARENT,PRODUCT_UNIT_SIZE,TAX,COMMISSION
        Iterable<Entity> allEntities = Datastore.getAllEntities("PRODUCT_DATA");
        for(Entity en : allEntities){
            String prodType = en.getProperty("PRODUCT_TYPE").toString();
            en.setProperty("PRODUCT_CATEGORY", prodType);
            en.setProperty("PRODUCT_SUB_CATEGORY",prodType);
            en.setProperty("TAX",0.0);
            en.setProperty("COMMISSION", 0.0);
            en.setProperty("PRODUCT_UNIT_SIZE",1.0);
            en.setProperty("PRODUCT_PARENT","");
            en.removeProperty("PRODUCT_TYPE");
            Datastore.insert(en);
        }
        worker.setResponseData(Message.SUCCESS);
        serv.messageToClient(worker);
    }
}
