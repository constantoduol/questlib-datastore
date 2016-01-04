package com.quest.servlets;

import com.quest.access.common.io;
import com.quest.access.control.Server;
import com.quest.access.useraccess.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/** 
 *
 * @author connie
 */


public class ServerLink extends HttpServlet {
    
    private static Server server;
    
    private static HashMap<String,String> requestMappings;
    

    /** 
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            response.setContentType("text/html;charset=UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");
            String json = request.getParameter("json");
            HttpSession session = request.getSession();
            //session, request, response
            JSONObject obj = new JSONObject();
            JSONObject requestData;
            JSONObject headers = new JSONObject();
            String msg,sessionId,service,endpoint;
            if (json != null) { //here we are dealing with json
                obj = new JSONObject(json);
                headers = obj.optJSONObject("request_header");
                msg = headers.optString("request_msg");
                sessionId = headers.optString("session_id");
                service = headers.optString("request_svc");
                endpoint = headers.optString("endpoint");
                requestData = (JSONObject) obj.optJSONObject("request_object");
            }
            else { 
                //here we are dealing with a url string e.g name=me&age=20
                //json is null check for other parameters and build the required 
                //request
                //check for svc, msg, ses_id
                service = request.getParameter("svc");
                msg = request.getParameter("msg");
                sessionId = request.getParameter("ses_id");
                endpoint = request.getParameter("endpoint");
                headers.put("request_msg", msg);
                headers.put("session_id", sessionId);
                headers.put("request_svc", service);
                headers.put("endpoint", endpoint);
                Map<String, String[]> paramz = request.getParameterMap();
                HashMap<String, String[]> params = new HashMap(paramz);
                params.remove("svc");
                params.remove("msg");
                params.remove("ses_id");
                Iterator iter = params.keySet().iterator();
                while(iter.hasNext()){
                    String key = iter.next().toString();
                    String [] param = params.get(key);
                    if(param.length == 1){
                        obj.put(key, param[0]);
                    }
                    else {
                        JSONArray arr = new JSONArray();
                        for(String value : param){
                            arr.put(value);
                        }
                       obj.put(key, arr);
                    }
                }
                requestData = obj;
            }
            
            ClientWorker worker = new ClientWorker(msg, service,headers,requestData, session, response, request,endpoint);
            worker = resolveRequestMappings(worker); //include endpoints for common functions e.g login,logout
    
            if (!authRequired(service, worker) || isAuthValid(sessionId)) {
                worker.work();
            } else {
                String value = "to use this service you need a valid auth token";
                sendMessage(response, "auth_required", value);
            }
        } catch (Exception ex) {
            Logger.getLogger(ServerLink.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private boolean isAuthValid(String sessionId) throws JSONException{
        ConcurrentHashMap<String, HttpSession> sessions = Server.getUserSessions();
        return  sessionId != null && sessions.containsKey(sessionId);
    }
    
    //create a mapping between a service and the remote server the service is located
    private void createRequestMappings(String mappings){
        HashMap<String,String> allMappings = new HashMap();
        StringTokenizer st = new StringTokenizer(mappings,",");
        while(st.hasMoreTokens()){
            String mapping = st.nextToken();
            StringTokenizer mt = new StringTokenizer(mapping,"|");
            int count = 0;
            String systemName = "";
            while(mt.hasMoreTokens()){
                if(count == 0){
                    systemName = mt.nextToken().trim();
                }
                else {
                    allMappings.put(mt.nextToken().trim(), systemName);
                }
                count++;
            }
        }
        requestMappings = allMappings;
    }
    
    private void registerSystems(String sys){
        ConcurrentHashMap systems = new ConcurrentHashMap();
        StringTokenizer st = new StringTokenizer(sys,",");
        while(st.hasMoreTokens()){
            String system = st.nextToken();
            int index = system.indexOf("|");
            String systemName = system.substring(0,index);
            String endpoint = system.substring(index + 1,system.length());
            systems.put(systemName.trim(), endpoint.trim());
        }
        //io.out(systems);
        server.setRegisteredSystems(systems);
    }
    
    private ClientWorker resolveRequestMappings(ClientWorker worker){
        //here we map requests to the corresponding servers
        //for example account related functions happen at https://quest-accounts.appspot.com
        //for example finance related functions happen at https://quest-pay.appspot.com
        //we have bundled all account related functionality in accounts_service and privileged_accounts_service
        //we have bundled all finance related functionality in payments_service and privileged_payments_service
        String sysName = requestMappings.get(worker.getService());
        if(sysName != null) worker.setEndpoint(server.getRegisteredSystems().get(sysName));
        return worker;
    }
    
    private boolean authRequired(String services, ClientWorker worker) {
        StringTokenizer st = new StringTokenizer(services, ",");
        HashMap allServices = Server.getServices();
        while (st.hasMoreTokens()) {
            String service = st.nextToken();
            if (allServices != null) {
                ArrayList serviceList = (ArrayList) allServices.get(service);
                if (serviceList != null) {
                    String privState = serviceList.get(2).toString();
                    Service svc = (Service) serviceList.get(3);
                    svc.runOnPreExecute(server, worker);
                    if (privState.equals("yes")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    
    /**
     *
     * @param response
     * @param msgKey
     * @param msgValue
     */
    public static void sendMessage(HttpServletResponse response,String msgKey, String msgValue){
        try {
            JSONObject object = new JSONObject();  
            object.put("request_msg",msgKey);
            object.put("data",msgValue);
            response.getWriter().print(object);
        } catch (Exception ex) {
            Logger.getLogger(ServerLink.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String [] args){
        String str = "quest_uza|https://test-quest-uza.appspot.com/server,\n" +
"                quest_accounts|https://quest-access.appspot.com/server,\n" +
"                quest_pay|https://quest-pay.appspot.com/server";
        ServerLink link = new ServerLink();
        link.registerSystems(str);
    }
 
    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /** 
     * Handles the HTTP <code>GET</code> method.
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /** 
     * Handles the HTTP <code>POST</code> method.
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /** 
     * Returns a short description of the servlet.
     * @return a String containing servlet description
     */ 
    @Override
    public String getServletInfo() {
        return "Entry point to the server";
    }// </editor-fold>
    
    /**
     *
     */
    @Override 
    public void init(){
         ServletConfig config = getServletConfig();
         String passExpires = config.getInitParameter("password-expires");
         String maxRetries = config.getInitParameter("max-password-retries");
         String clientTimeout = config.getInitParameter("client-timeout");
         String mLogin = config.getInitParameter("multiple-login");
         String defPass = config.getInitParameter("default-password");
         String rootUser = config.getInitParameter("root-user");
         String debug = config.getInitParameter("debug-mode");
         String systemName = config.getInitParameter("system-name");
         String mappings = config.getInitParameter("request-mappings");
         String systems = config.getInitParameter("registered-systems");
         try {
            server = new Server(systemName);
            if(debug.equals("true")){
                server.setDebugMode(true);
            }
            else{
                server.setDebugMode(false);
            }
            server.setPassWordLife(Integer.parseInt(passExpires));
            server.setMaxPasswordAttempts(Integer.parseInt(maxRetries));
            server.setClientTimeout(Double.parseDouble(clientTimeout));
            server.setMultipleLoginState(Boolean.parseBoolean(mLogin));
            server.setConfig(config);
            server.setDefaultPassWord(defPass);
            server.createRootUser(rootUser);
            server.startAllServices();
            createRequestMappings(mappings);
            registerSystems(systems);
        } catch (Exception ex) {
            io.log(ex, Level.SEVERE, this.getClass());
        }
    }
    
    /**
     *
     * @return
     */
    public static Server getServerInstance(){
        return server;
    }
    
  
}
