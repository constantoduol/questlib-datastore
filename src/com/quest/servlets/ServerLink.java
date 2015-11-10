/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.quest.servlets;

import com.quest.access.common.io;
import com.quest.access.control.Server;
import com.quest.access.useraccess.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.json.JSONArray;
import org.json.JSONObject;


/** 
 *
 * @author connie
 */


public class ServerLink extends HttpServlet {
    
    private static Server server;

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
            String msg,sessionId,service;
            if (json != null) { //here we are dealing with json
                obj = new JSONObject(json);
                JSONObject headers = obj.optJSONObject("request_header");
                msg = headers.optString("request_msg");
                sessionId = headers.optString("session_id");
                service = headers.optString("request_svc");
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
            ConcurrentHashMap<String, HttpSession> sessions = Server.getUserSessions();
            boolean authValid = sessionId != null && sessions.containsKey(sessionId);
            ClientWorker worker = new ClientWorker(msg, service, requestData, session, response, request);
            if (!authRequired(service, worker) || authValid) {
                worker.work();
            } else {
                String value = "to use this service you need a valid auth token";
                sendMessage(response, "auth_required", value);
            }
        } catch (Exception ex) {
            Logger.getLogger(ServerLink.class.getName()).log(Level.SEVERE, null, ex);
        }
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
    
    @Override 
    public void init(){
         ServletConfig config = getServletConfig();
         String serverName=config.getInitParameter("database-name");
         String passExpires=config.getInitParameter("password-expires");
         String maxRetries=config.getInitParameter("max-password-retries");
         String clientTimeout=config.getInitParameter("client-timeout");
         String mLogin=config.getInitParameter("multiple-login");
         String defPass=config.getInitParameter("default-password");
         String rootUser = config.getInitParameter("root-user");
         String debug = config.getInitParameter("debug-mode");
         try {
            server = new Server(serverName);
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
        } catch (Exception ex) {
        	io.log(ex, Level.SEVERE, this.getClass());
      } 
    }
 
    public static Server getServerInstance(){
        return server;
    }
    
  
}
