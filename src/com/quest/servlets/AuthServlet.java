/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.quest.servlets;

import com.quest.access.common.io;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;


/** 
 *
 * @author connie
 */


public class AuthServlet extends HttpServlet {
    
    /** 
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String authType = request.getParameter("type");
        if(authType == null){
          return;
        }
        else if(authType.equals("login")){
           writeLogin(response,"");
        }
        else if(authType.equals("change")){
           writeChangePass(response,"");
        }
        else if(authType.equals("auth")){
        	doAuth(request,response);
        }
        else if(authType.equals("change_auth")){
        	doChangeAuth(request,response);
        }
    }
    
    private String getURI(HttpServletRequest request){
        String uri = request.getScheme() + "://" + // "http" + "://
                request.getServerName() + // "myhost"
                ":" + // ":"
                request.getServerPort();// "8080"
        return uri;
    }
    
    private void doChangeAuth(HttpServletRequest req,HttpServletResponse resp){
        try {
      	String name = req.getParameter("username");
      	String oldpass = req.getParameter("old_password");
      	String newpass = req.getParameter("new_password");
      	JSONObject obj = new JSONObject();
      	JSONObject rHeader = new JSONObject();
      	JSONObject rObject = new JSONObject();
      	rHeader.put("request_msg", "changepass");
      	rObject.put("user_name",name);
      	rObject.put("old_password",oldpass);
      	rObject.put("new_password",newpass);
      	obj.put("request_header",rHeader);
      	obj.put("request_object", rObject);
      	JSONObject remote = sendRemoteData(obj,getURI(req)+"/server");
      	String response = remote.optJSONObject("response").optString("response");
      	writeChangePass(resp,remote.toString());
      	
        }
        catch(Exception e){
      	  
        }
      }
    
    private void doAuth(HttpServletRequest req,HttpServletResponse resp){
      try {
    	String name = req.getParameter("username");
    	String pass = req.getParameter("password");
    	JSONObject obj = new JSONObject();
    	JSONObject rHeader = new JSONObject();
    	JSONObject rObject = new JSONObject();
    	rHeader.put("request_msg", "login");
    	rObject.put("username",name);
    	rObject.put("password",pass);
    	obj.put("request_header",rHeader);
    	obj.put("request_object", rObject);
        io.out(getURI(req));
    	JSONObject remote = sendRemoteData(obj,getURI(req)+"/server");
    	String response = remote.optJSONObject("response").optString("response");
    	writeLogin(resp,remote.toString());
    	
      }
      catch(Exception e){
    	  
      }
    }
    
    private static JSONObject sendRemoteData(JSONObject data,String remoteUrl){
    	try {
    		String urlParams = URLEncoder.encode("json", "UTF-8") + "=" + URLEncoder.encode(data.toString(), "UTF-8");
            URL url = new URL(remoteUrl);
            URLConnection conn = url.openConnection();
            conn.setRequestProperty("Accept", "application/json");
            HttpURLConnection httpConn = (HttpURLConnection) conn;
            httpConn.setRequestMethod("POST");
            httpConn.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(httpConn.getOutputStream());
            wr.writeBytes(urlParams);
            wr.flush();
            wr.close();
            int responseCode = httpConn.getResponseCode();
            BufferedReader reader;
            if(responseCode == 200) {
                reader = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
                String inputLine = reader.readLine();
                reader.close();
                return new JSONObject(inputLine);
            } else {
               return null;
            }
        } catch (Exception ex) {
            Logger.getLogger(AuthServlet.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
  }
    
    private void writeLogin(HttpServletResponse resp,String msg){
      try {
		PrintWriter writer = resp.getWriter();
		writer.println("<!DOCTYPE html>");
		writer.println("<html>");
		writer.println("<head>");
	
		writer.println("<title>Quest Access Login</title>");
		writer.println("<meta http-equiv='Content-Type' content='text/html; charset=UTF-8'>");
		writer.println("<style>");
		writer.println(".login-form {"+
            "max-width: 300px;"+
            "padding: 19px 29px 29px;"+
            "margin: 0 auto 20px;"+
            "background-color: #fff;"+
            "border: 1px solid #51CBEE;"+
            "-webkit-border-radius: 5px;"+
            "-moz-border-radius: 5px;"+
            "border-radius: 5px;"+
            "-webkit-box-shadow: 0 1px 2px rgba(0,0,0,.05);"+
            "-moz-box-shadow: 0 1px 2px rgba(0,0,0,.05);"+
            "box-shadow: 0 1px 2px rgba(0,0,0,.05);"+
            "font-family:arial;"+
            "text-decoration:none;"+
        "}"+
        ".login-form input[type='text'],"+
        ".login-form input[type='password'] {"+
            "font-size: 16px;"+
            "height: auto;"+
            "margin-bottom: 15px;"+
            "padding: 7px 9px;"+
        "}"+
        ".btn {"+
        "background-color:#4169E1;"+
        "display: inline-block;"+
        "margin-bottom: 0;"+
        "font-weight: 400;"+
        "text-align: center;"+
        "vertical-align: middle;"+
        "cursor: pointer;"+
        "background-image: none;"+
        "border: 1px solid transparent;"+
        "white-space: nowrap;"+
        "padding: 6px 12px;"+
        "font-size: 14px;"+
        "line-height: 1.42857143;"+
        "border-radius: 4px;"+
        "-webkit-user-select: none;"+
        "}"+
        ".footer{"+
           "background: #eee;"+
           "position: absolute;"+
           "bottom: 0;"+
           "padding: 5px;"+
           "width : 99%;}");
		writer.println("</style>");
		writer.println("</head>");
		writer.println("<body style='padding-top: 50px'>");
		writer.println("<form method='POST' action='/auth?type=auth'>");
		writer.println("<div class='login-form'>");
		writer.println("<h3>Login</h3>");
		writer.println("<label>Username</label>");
		writer.println("<input type='text' class='input-block-level' name='username' placeholder='Username'  />");
		writer.println("<label>Password</label>");
		writer.println("<input type='password' class='input-block-level' name='password' placeholder='Password' />");
		
		writer.println("<input type='submit' class='btn btn-primary pull-right' value='Log in' />");                
		writer.println("<div style='clear: both'></div><br>");
		writer.println("<footer>");
		writer.println("<label style='color : red;'>"+msg+"</label><br>");
		writer.println("<a href='/auth?type=change'>Change Password</a>");
		writer.println("<div style='clear: both'></div>");
		writer.println("</footer>");
		writer.println("</div>");
		writer.println("</form>");  
		    
		writer.println("</body>");
		writer.println("</html>");
		writer.flush();
		writer.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
      
    }
    
    
    private void writeChangePass(HttpServletResponse resp,String msg){
        try {
  		PrintWriter writer = resp.getWriter();
  		writer.println("<!DOCTYPE html>");
  		writer.println("<html>");
  		writer.println("<head>");
  	
  		writer.println("<title>Quest Access Change Password</title>");
  		writer.println("<meta http-equiv='Content-Type' content='text/html; charset=UTF-8'>");
  		writer.println("<style>");
  		writer.println(".login-form {"+
              "max-width: 300px;"+
              "padding: 19px 29px 29px;"+
              "margin: 0 auto 20px;"+
              "background-color: #fff;"+
              "border: 1px solid #51CBEE;"+
              "-webkit-border-radius: 5px;"+
              "-moz-border-radius: 5px;"+
              "border-radius: 5px;"+
              "-webkit-box-shadow: 0 1px 2px rgba(0,0,0,.05);"+
              "-moz-box-shadow: 0 1px 2px rgba(0,0,0,.05);"+
              "box-shadow: 0 1px 2px rgba(0,0,0,.05);"+
              "font-family:arial;"+
              "text-decoration:none;"+
          "}"+
          ".login-form input[type='text'],"+
          ".login-form input[type='password'] {"+
              "font-size: 16px;"+
              "height: auto;"+
              "margin-bottom: 15px;"+
              "padding: 7px 9px;"+
          "}"+
          ".btn {"+
          "background-color:#4169E1;"+
          "display: inline-block;"+
          "margin-bottom: 0;"+
          "font-weight: 400;"+
          "text-align: center;"+
          "vertical-align: middle;"+
          "cursor: pointer;"+
          "background-image: none;"+
          "border: 1px solid transparent;"+
          "white-space: nowrap;"+
          "padding: 6px 12px;"+
          "font-size: 14px;"+
          "line-height: 1.42857143;"+
          "border-radius: 4px;"+
          "-webkit-user-select: none;"+
          "}"+
          ".footer{"+
             "background: #eee;"+
             "position: absolute;"+
             "bottom: 0;"+
             "padding: 5px;"+
             "width : 99%;}");
  		writer.println("</style>");
  		writer.println("</head>");
  		writer.println("<body style='padding-top: 50px'>");
  		writer.println("<form method='POST' action='/auth?type=change_auth'>");
  		writer.println("<div class='login-form'>");
  		writer.println("<h3>Change Password</h3>");
  		writer.println("<label>Username</label><br>");
  		writer.println("<input type='text' class='input-block-level' name='username' placeholder='Username'  />");
  		writer.println("<br><label>Old Password</label>");
  		writer.println("<input type='password' class='input-block-level' name='old_password' placeholder='Old Password' />");
  		writer.println("<br><label>New Password</label>");
  		writer.println("<input type='password' class='input-block-level' name='new_password' placeholder='New Password' />");
  		writer.println("<input type='submit' class='btn btn-primary pull-right' value='Change Password' />");                
  		writer.println("<div style='clear: both'></div><br><br>");
  		writer.println("<footer>");
  		writer.println("<label style='color : red;'>"+msg+"</label>");
  		writer.println("<div style='clear: both'></div>");
  		writer.println("</footer>");
  		writer.println("</div>");
  		writer.println("</form>");  
  		    
  		writer.println("</body>");
  		writer.println("</html>");
  		writer.flush();
  		writer.close();
  	} catch (IOException e) {
  		// TODO Auto-generated catch block
  		e.printStackTrace();
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
        return "Authentication Servlet";
    }// </editor-fold>
    
    @Override 
    public void init(){
    
    } 
    
  

}
