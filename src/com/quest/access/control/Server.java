package com.quest.access.control;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpSession;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.quest.access.common.UniqueRandom;
import com.quest.access.common.io;
import com.quest.access.common.datastore.Datastore;
import com.quest.access.crypto.Security;
import com.quest.access.useraccess.NonExistentUserException;
import com.quest.access.useraccess.Service;
import com.quest.access.useraccess.Serviceable;
import com.quest.access.useraccess.User;
import com.quest.access.useraccess.services.annotations.Endpoint;
import com.quest.access.useraccess.services.annotations.WebService;
import com.quest.access.useraccess.verification.SystemAction;
import com.quest.mail.SendGrid;
import com.quest.mail.SendGridException;
import com.quest.servlets.ClientWorker;

/**
 *
 * @author constant oduol
 * @version 1.0(4/1/2012)
 */
/**
 * This file defines a server When a new server is created a new database is
 * created along with it the database has the following tables
 * <p>
 * USERS- this table contain details of users on this server PRIVILEGES- this
 * table contains privileges of all the users on this server RESOURCE_GROUPS-
 * this table stores the permanent privileges on this server RESOURCES- this
 * table stores the resources on this server USER_HISTORY- this table stores
 * details of deleted users LOGIN- this table stores the login details of a user
 * for every new login LOGOUT- this table stores the logout details of a user
 * for every logout SERVICES- this table stores the services registered on this
 * server
 * </p>
 * <p>
 * when an instance of a server is created it starts listening for client
 * connections on the port specified during its creation when a client connects
 * the server sends the client a new request for the client to login, the client
 * needs to respond by sending a response with the login details. The server
 * then tries to log in the user, the server then responds to the client with
 * the login status
 * </p>
 *
 * <p>
 * Clients can send requests to a server and a servers can sent requests to
 * clients A server has the method processRequest() which when overriden can be
 * used to process requests from clients. When clients send a request for a
 * service the server class invokes the required service through the private
 * method processClientRequest()
 * </p>
 *
 * <p>
 * A server has a privilege handler class that ensures that only users that have
 * the required privileges access services on the server, users with no
 * privileges have an security exception object returned back to the client to
 * show that the client was denied access to the privilege he was not
 * assigned.Also, once a client logs out of the system, if he tries to access
 * any service a security exception is sent to the client in a response object
 * therefore clients should check the message in a response object if it equals
 * "exception" so as to handle exceptions send by the server
 * </p>
 *
 * <p>
 * Clients can make standard requests to the server sending a request with the
 * following messages makes the server respond as specified
 * <p>
 * logoutuser- this asks the server to log out the user accessing the server
 * through the client that sent the request, sending this request requires the
 * client to send the users username along with the message e.g. new
 * Request(userName,"logoutuser");
 * </p>
 * <p>
 * logoutclient- this asks the server to log out the user accessing the server
 * through the client that sent the request, sending this request does not
 * require the user's user name e.g. new Request("logoutclient");
 * </p>
 * <p>
 * forcelogout- this asks the server to mark the user in the database as logged
 * out, the user name of the user to be marked as logged out is sent in the
 * request object e.g. new Request(userName,"forcelogout");
 * </p>
 * </p>
 *
 * <p>
 * When a user logs in to a server a new Session is created for that user a
 * session has several attributes predefined by the server if ses is an instance
 * of a user session then ses.getAttribute("attributename") returns the required
 * attribute
 * <ol>
 * <li>clientid - this is the id of the connected client</li>
 * <li>username - this is the username of the connected client</li>
 * <li>host - this is the host from which the client is connecting</li>
 * <li>clientip - this is the ip address of the client machine</li>
 * <li>privileges - this is a hashmap containing the privileges of the user</li>
 * <li>userid - this is a string representing the twenty digit system generated
 * id</li>
 * <li>superiority - this is a double value representing the user's
 * superiority</li>
 * <li>created - this is a date object representing when the user was
 * created</li>
 * <li>group - this is the group that the user belongs to or "unassigned" if the
 * user does not belong to any group</li>
 * <li>loginid - this is the system generated id representing the user's most
 * recent login</li>
 * <li>lastlogin - this is the system generated id representing the user's
 * previous login</li>
 * <li>sessionstart - this is a date object representing when this user's
 * session started</li>
 * </ol>
 * </p>
 *
 * <p>
 * The LOGIN table contains details about user logins, the user name, client ip,
 * server ip, and time of login are stored in the login table,similarly the
 * LOGOUT table contains details about successful logouts that is the user name,
 * client ip, server ip and logout time.Login and logout from one session by a
 * client is marked by one id i.e the login id is the same as the logout id for
 * any user session
 * </p>
 *
 * <p>
 * During user login the server normally returns messages depending on the
 * status of the login these messages can be obtained from the returned response
 * object by calling the getResponse() method
 * <ol>
 * <li>notexist - the server returns this response if the user attempting to log
 * in does not exist</li>
 * <li>disabled - the server returns this if the user attempting to log in has
 * his account disabled</li>
 * <li>loggedin - the server returns this if the user attempting to log in is
 * already logged in</li>
 * <li>loginsuccess - the server returns this if the user has been successfully
 * logged in</li>
 * <li>invalidpass - the server returns this if the user trying to log in has a
 * valid username but invalid password</li>
 * <li>changepass - the server returns this if a user's password is expired or
 * for a new user in order for the user to change his password</li>
 * <li>maxpassattempts - this message is sent to inform the client that he has
 * reached the maximum allowed password attempts</li>
 * </ol>
 * </p>
 */
public class Server {

    private static ConcurrentHashMap<String, HttpSession> sessions = new ConcurrentHashMap();

    private double clientTimeout;
    /*
     * this variable tells us how many times a user can retry logging in after
     * failing
     */

    private Integer maxPasswordRetries;
    /*
     * this contains the services available to the server
     */
    private static HashMap<String, ArrayList> services;

    /*
     * this variable controls whether the server prints out error messages or
     * not
     */
    private boolean debugmode;

    /*
     * this variable keeps track of users last request to the server
     */
    private ConcurrentHashMap<String, Long> lastClientRequest = new ConcurrentHashMap();

    /*
     * this variable keeps track of login attempts by users
     */
    private final ConcurrentHashMap<String, Integer> loginAttempts = new ConcurrentHashMap();

    /*
     * this is the default password for this server
     */
    private String defaultPass;

    /*
     * this is the directory containing extra classes used by this server
     */
    private String dir;
    /*
     * this tells us how long user passwords take to expire
     */
    private int passwordLife;

    /*
     * this tells us whether the super group allows multiple logins
     */
    private boolean multipleLogins;

    /*
     * This is the name of the super group
     */
    private String name;

    private String rootUser;

    /*
     * this hashmap contains instances of started services
     */
    private final ConcurrentHashMap<String, Object> runtimeServices;

    /*
     * this hashmap contains mappings of message names to their respective
     * methods
     */
    private final ConcurrentHashMap<String, Method> serviceRegistry;

    private ServletConfig config;

    /*
     * contains information about method sharing between services
     */
    private ConcurrentHashMap<String, Object[]> sharedRegistry;
    /**
     * the key is the root worker id and the value is an array of client workers
     */
    private ConcurrentHashMap<String, ClientWorker[]> rootWorkers;

    /*
     * This are tables the user can request data from safely from the frontend
     * without a security breach
     */
    private static final HashMap<String, JSONArray> safeEntities = new HashMap();

    public Server(String name) {
        this.name = name;
        this.multipleLogins = false;
        this.runtimeServices = new ConcurrentHashMap();
        this.serviceRegistry = new ConcurrentHashMap();
        this.sharedRegistry = new ConcurrentHashMap<>();
        this.rootWorkers = new ConcurrentHashMap<>();
        this.passwordLife = 1440;
        this.maxPasswordRetries = 0;
    }

    /**
     * we add columns that we consider safe for users to invoke from the front
     * end i.e a user can invoke arbitrary sql on this tables and columns
     * without posing a security breach e.g accessing a passwords column etc
     *
     * @param database the database where the column is contained
     * @param table the table where the column is contained
     * @param column the column to be marked as safe for querying directly from
     * the frontend
     */
    public void addSafeEntity(String entity, JSONArray columns) {
        JSONArray cols = safeEntities.get(entity);
        if (cols == null) {
            safeEntities.put(entity, columns);
        } else {
            JSONArray current = safeEntities.get(entity);
            for (int x = 0; x < columns.length(); x++) {
                current.put(columns.optString(x));
            }

        }
    }

    public boolean isEntitySafe(String entity, JSONArray columns) {
        JSONArray cols = safeEntities.get(entity);
        if (cols != null) {
            if (cols.toList().contains("*")) {
                return true;
            } else {
                return columns.toList().containsAll(columns.toList());
            }
        }
        return false;
    }

    /**
     * this method sets the password life of this server in minutes
     */
    public void setPassWordLife(int life) {
        this.passwordLife = life;
    }

    /**
     * this method returns the password life of this server in minutes
     */
    public int getPassWordLife() {
        return this.passwordLife;
    }

    /**
     * this method tells us how many times a user in this server can retry
     * logging in after failing on the first attempt, note the first attempt is
     * also counted as an attempt, therefore if the maximum attempts are 3, if a
     * user fails on the first attempt only two attempts remain
     */
    public Integer getMaxPasswordAttempts() {
        return this.maxPasswordRetries;
    }

    /**
     * this method sets the maximum password attempts for this server
     *
     * @param attempts this is the number of attempts a user can try logging in
     * to the server
     */
    public void setMaxPasswordAttempts(Integer attempts) {
        this.maxPasswordRetries = attempts;
    }

    /**
     * this method returns the default password of this server
     */
    public String getDefaultPassWord() {
        if (this.defaultPass == null) {
            return "pass";
        }
        return this.defaultPass;
    }

    public void setConfig(ServletConfig config) {
        this.config = config;
    }

    public ServletConfig getConfig() {
        return this.config;
    }

    /*
     * if this is true the server will print stacktraces on errors
     */
    public void setDebugMode(boolean mode) {
        this.debugmode = mode;
    }

    /**
     * this method sets the default password
     */
    public void setDefaultPassWord(String pass) {
        this.defaultPass = pass;
    }

    /**
     * this method returns the time taken for the server to time out a client in
     * minutes
     */
    public double getClientTimeout() {
        return this.clientTimeout;
    }

    /**
     * this method sets the time taken for the server to time out a client
     *
     * @param time the time taken in minutes for the server to time out a client
     */
    public void setClientTimeout(double time) {
        this.clientTimeout = time;
    }

    /**
     * this method tells us whether multiple logins of users are allowed
     */
    public void setMultipleLoginState(boolean state) {
        this.multipleLogins = state;
    }

    /**
     * this method tells us whether multiple logins are allowed
     */
    public boolean getMultipleLoginState() {
        return this.multipleLogins;
    }

    /**
     * this method returns the name of the server
     */
    public String getName() {
        return this.name;
    }

    public String getRootUser() {
        return this.rootUser;
    }

    public final void createRootUser(String rootUser) {
        try {
            this.rootUser = rootUser;
            String externalServices = config.getInitParameter("external-services");
            initExternalServices(externalServices);
            String initType = this.config.getInitParameter("init-type");
            if (!initType.equals("initial")) return;
            SystemAction action2 = new SystemAction("CREATE_USER " + rootUser);
            User user = new User(rootUser, this.getDefaultPassWord(), "localhost", rootUser, action2);
        } catch (Exception ex) {
            io.log(ex, Level.SEVERE, Server.class);
        }
    }

    /**
     * returns a string representation of a server in this format Server[name:
     * id]
     */
    @Override
    public String toString() {
        return "Server[" + this.name + "]";
    }

    /**
     * this method gets all the services belonging to a server in a hash map
     * with the key as the service name and the value as the location of the
     * service class
     *
     * @param serv the server in which this service is meant to be accessed by
     * clients
     */
    public static HashMap<String, ArrayList> getServices() {
        return services;
    }

    /**
     * This method starts all the services created on the database and creates a
     * registry mapping message requests to the appropriate methods
     */
    public void startAllServices() {
        String initUsers = config.getInitParameter("grant-init-users");
        String initPrivs = config.getInitParameter("grant-init-privileges");
        grantInitPrivileges(initUsers, initPrivs);

    }

    public String getEmailTemplate(String name) {
        try {
            InputStream in = getClass().getResourceAsStream("/com/quest/mail/" + name + ".html");
            return IOUtils.toString(in, "utf-8");
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
            return "";
        }
    }

    /**
     *
     * @param from contains the email and preferred name of the sender
     * @param to contains the email and preferred name of recipient
     * @param subject contains the subject of the email to be sent
     * @param msgBody this is the message to send
     */
    public void sendEmail(String[] from, String[] to, String subject,String msgBody) {
        String userName = this.config.getInitParameter("sendgrid-username");
        String pass = this.config.getInitParameter("sendgrid-password");
        SendGrid sendgrid = new SendGrid(userName, pass);
        SendGrid.Email email = new SendGrid.Email();
        email.addTo(to[0]);
        email.addToName(to[1]);
        email.setFrom(from[0]);
        email.setFromName(from[1]);
        email.setSubject(subject);
        email.setHtml(msgBody);
        try {
            SendGrid.Response response = sendgrid.send(email);
            System.out.println(response.getMessage());
        } catch (SendGridException e) {
            System.err.println(e);
        }
    }

    private void grantInitPrivileges(String users, String privileges) {
        String initType = this.config.getInitParameter("init-type");
        if (!initType.equals("initial")) return;
        StringTokenizer privs = new StringTokenizer(privileges, ",");
        StringTokenizer userz = new StringTokenizer(users, ",");
        ArrayList<String> list = new ArrayList();
        while (privs.hasMoreTokens()) {
            String priv = privs.nextToken().trim();
            list.add(priv);
        }
        String[] permArr = new String[list.size()];
        for (int x = 0; x < list.size(); x++) {
            permArr[x] = list.get(x);
        }
        while (userz.hasMoreTokens()) {
            String userName = userz.nextToken();
            try {
                User user = User.getExistingUser(userName);
                user.grantPrivileges(permArr);
            } catch (NonExistentUserException ex) {

            }
        }

    }

    private void initExternalServices(String services) {
        StringTokenizer token = new StringTokenizer(services, ",");
        HashMap serviceMap = new HashMap();
        while (token.hasMoreTokens()) {
            try {
                String serviceLocation = token.nextToken().trim();
                Class serviceClass = Class.forName(serviceLocation.trim());
                WebService webService = (WebService) serviceClass.getAnnotation(WebService.class);
                if (webService != null) {
                    ArrayList values = new ArrayList();
                    int level = webService.level();
                    String serviceName = webService.name();
                    Service service = new Service(serviceName, serviceClass, this);
                    values.add(serviceLocation);
                    values.add(level);
                    values.add(webService.privileged());
                    values.add(service);
                    serviceMap.put(serviceName, values);
                }
            } catch (Exception ex) {
                java.util.logging.Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        Server.services = serviceMap;
    }

    public void startService(String serviceLocation) {
        try {
            Class serviceClass = Class.forName(serviceLocation);
            Object newInstance = serviceClass.newInstance();
            runtimeServices.put(serviceLocation, newInstance);
            registerMethods(serviceClass);
            Method method = serviceClass.getMethod("onStart", new Class[]{Server.class});
            method.invoke(newInstance, new Object[]{this});
            io.log("Service " + serviceLocation + " Started successfully ", Level.SEVERE, Server.class);
        } catch (Exception e) {
            io.log("Error starting service " + serviceLocation + ": " + e, Level.SEVERE, Server.class);
            e.printStackTrace();
        }
    }

    /**
     * this method registers message name mappings to methods
     */
    private void registerMethods(Class<?> serviceClass) {
        // io.log("Registering methods for service: "+serviceClass.getName(),Level.INFO,
        // Server.class);
        try {
            Method[] methods = serviceClass.getDeclaredMethods();
            for (Method method : methods) {
                Endpoint endpoint = method.getAnnotation(Endpoint.class);
                if (endpoint != null) {
                    String message = endpoint.name();
                    String key = message + "_" + serviceClass.getName();
                    serviceRegistry.put(key, method);
                    String[] shareWith = endpoint.shareMethodWith();
                    for (String shareWith1 : shareWith) {
                        String shareKey = message + "_" + shareWith1; // all_fields_mark_service
                        sharedRegistry.put(shareKey, new Object[]{message,
                            serviceClass.getName()});
                        // System.out.println( "share_key: "+shareKey
                        // +"  share_data : "+Arrays.toString( new
                        // Object[]{message,serviceClass} ));
                    }
                }
            }
        } catch (Exception e) {
            io.log(e, Level.SEVERE, Server.class);
        }
    }

    /**
     * this method sends a synchronous request to a remote url and awaits the
     * response the returned data is in json format
     *
     * @param requestData the data passed to the remote url
     * @param remoteUrl the remote url e.g https://10.1.10.190:8080/web/server
     * @return a json object with the data returned by the url
     */
    public static JSONObject remote(Object requestData, String remoteUrl) {
        try {
            String urlParams = URLEncoder.encode("json", "UTF-8") + "="
                    + URLEncoder.encode(requestData.toString(), "UTF-8");
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
            if (responseCode == 200) {
                reader = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
                String inputLine = reader.readLine();
                reader.close();
                return new JSONObject(inputLine);
            } else {
                return null;
            }
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    /**
     * this method processes a request by a client for a service
     * @param worker the id of the client who sent this request
     */
    public void processClientRequest(ClientWorker worker) {
        try {
            HashMap serviceList = this.services;
            String service = worker.getService();
            ArrayList values = (ArrayList) serviceList.get(service);
            if (values != null) {
                try {
                    String location = (String) values.get(0);
                    // TODO make more service instances available in future
                    Object serviceInstance = this.runtimeServices.get(location); // we																
                    Serviceable serviceProx = (Serviceable) this.proxify(serviceInstance, worker, service, location);
                    serviceProx.service();
                } catch (Exception e) {
                    // Logger.toConsole("An error occurred while invoking service: "+service+" Reason:"+e,
                    // Server.class);
                    worker.setResponseData(e);
                    exceptionToClient(worker);
                }
            } else {
                // Logger.toConsole("Service "+service+" not found on server",
                // Server.class);
                worker.setResponseData("Service " + service + " not found on server");
                messageToClient(worker);
            }
        } catch (Exception e) {
            worker.setResponseData(e);
            exceptionToClient(worker);
        }
    }

    /**
     * this method is used to ensure that a user on the front end can invoke
     * multiple services with multiple messages at the same time, if one of the
     * requests fail due to insufficient privileges all the requests fail also
     * e.g.
     *
     * <code>
     * <br>
     * Ajax.run({<br>
     * url : serverUrl,<br>
     * type : "post",<br>
     * data : {<br>
     * request_header : {<br>
     * request_msg : "all_streams",<br>
     * request_svc :"mark_service"<br>
     * }<br>
     * },<br>
     * error : function(err){<br>
     *
     * },<br>
     * success : function(json){<br>
     *
     * } <br>
     * }); <br>
     * </code> and <code>
     * <br>
     * Ajax.run({ <br>
     * url : serverUrl,<br>
     * type : "post",<br>
     * data : {<br>
     * request_header : {<br>
     * request_msg : "all_students",<br>
     * request_svc :"student_service"<br>
     * }<br>
     * },<br>
     * error : function(err){<br>
     *
     * },<br>
     * success : function(json){<br>
     *
     * } <br>
     * });
     * </code>
     *
     * can be combined to <code>
     * <br>
     * Ajax.run({ <br>
     * url : serverUrl,<br>
     * type : "post",<br>
     * data : {<br>
     * request_header : { <br>
     * request_msg : "all_streams, all_students", <br>
     * request_svc :"mark_service, student_service" <br>
     * } <br>
     * }, <br>
     * error : function(err){ <br>
     *
     * }, <br>
     * success : function(json){ <br>
     * var all_streams = json.data.response[0] <br>
     * var all_students = json.data.response[1] <br>
     * } <br>
     * });
     * </code> <br>
     * data is only sent back to the client after the last request is completed
     * if request one returns immediately but request two delays then the data
     * will be transmitted to the client after request two completes
     *
     * @param rootWorker
     */
    public void invokeMultipleServices(ClientWorker rootWorker) {

        String servicez = rootWorker.getService();
        String messagez = rootWorker.getMessage();
        StringTokenizer st = new StringTokenizer(servicez, ",");
        StringTokenizer st1 = new StringTokenizer(messagez, ",");

        if (st.countTokens() == 1) {
            processClientRequest(rootWorker); // there is only one service and
            // one message so just invoke
            // the required service
        } else {
            /*
             * here we have more than one service and one message e.g
             * request_msg : "all_streams, all_students", request_svc
             * :"mark_service, student_service" the strategy is to split the
             * root worker into many workers, we discard the root worker and
             * then service each slave worker individually, now when the first
             * slave worker responds we check to see whether its other slave
             * workers have responded, if its the last slave worker then we send
             * the response to the client we save the worker id and its data
             */
            ClientWorker[] workers = new ClientWorker[st.countTokens()];
            for (int x = 0; st.hasMoreTokens(); x++) {
                String service = st.nextToken().trim();
                String message = st1.nextToken().trim();
                ClientWorker worker = new ClientWorker(message, service,
                        rootWorker.getRequestData(), rootWorker.getSession(),
                        rootWorker.getResponse(), rootWorker.getRequest());
                worker.setRootWorkerID(rootWorker.getID());
                workers[x] = worker;
            }
            rootWorkers.put(rootWorker.getID(), workers);
            for (ClientWorker theWorker : workers) {
                processClientRequest(theWorker);
            }
        }

    }

    /**
     * this method resets the login attempts of a specific user so that the user
     * can try logging in again
     *
     * @param userName the name of the user we want to reset login attempts
     */
    public final void resetLoginAttempts(String userName) {
        loginAttempts.remove(userName);
    }

    /**
     * this method resets the time out value of a given user
     */
    public final void resetTimeout(String userName) {
        lastClientRequest.remove(userName);
    }

    public ConcurrentHashMap<String, Long> getTimeoutData() {
        return lastClientRequest;
    }

    /**
     * attributes specified in the session of each client clientid- this is the
     * id of the connected client username- this is the username of the
     * connected client host- this is the host from which the client is
     * connecting clientip- this is the ip address of the client machine these
     * attributes are obtained by getting an attribute on a session object and
     * specifying the name of the attribute
     *
     * @param clientID the id of the client we are serving
     * @param props the properties object sent by the client
     */
    private void createSession(ClientWorker worker, User theUser) {
        try {
            HttpSession ses = worker.getSession();
            JSONObject props = worker.getRequestData();
            String uName = theUser.getUserProperty("USER_NAME");
            sessions.put(ses.getId(), ses);
            String uHost = (String) props.optString("host");
            String clientIP = (String) props.optString("clientip");
            ses.setAttribute("username", uName);
            ses.setAttribute("host", uHost);
            ses.setAttribute("clientip", clientIP);
            Entity user = Datastore.getSingleEntity("USERS", "USER_NAME", uName, FilterOperator.EQUAL);
            Entity buss = Datastore.getSingleEntity("BUSINESS_USERS", "USER_NAME", uName, FilterOperator.EQUAL);
           
            String busId = buss.getProperty("BUSINESS_ID").toString();
            String userId = (String) user.getProperty("USER_ID");
            Long created = (Long) user.getProperty("CREATED");
            String group = (String) user.getProperty("GROUPS");
            Long lastLogin = (Long) user.getProperty("LAST_LOGIN");
            Date sessionStart = new Date();

            ses.setAttribute("userid", userId);
            ses.setAttribute("created", created);
            ses.setAttribute("group", group);
            ses.setAttribute("lastlogin", lastLogin);
            ses.setAttribute("sessionstart", sessionStart);
            ses.setAttribute("business_id",busId);

            JSONArray userPrivileges = theUser.getUserPrivileges();
            ses.setAttribute("privileges", userPrivileges);

            UniqueRandom ur = new UniqueRandom(30);
            String loginID = ur.nextMixedRandom();
            ses.setAttribute("loginid", loginID);
            String serverIP = InetAddress.getLocalHost().getHostAddress();
            String serverHost = InetAddress.getLocalHost().getHostName();

            Entity login = new Entity("LOGIN");
            login.setProperty("LOGIN_ID", loginID);
            login.setProperty("USER_NAME", uName);
            login.setProperty("LOGIN_TIME", System.currentTimeMillis());
            login.setProperty("SERVER_IP", serverIP);
            login.setProperty("SERVER_HOST", serverHost);
            login.setProperty("CLIENT_IP", clientIP);
            Datastore.insert(login);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ConcurrentHashMap<String, HttpSession> getUserSessions() {
        return sessions;
    }

    /**
     * this method logs in a user
     *
     * @param worker the id of the connected client
     */
    public void doLogin(ClientWorker worker) {
        try {
            JSONObject requestData = worker.getRequestData();
            String uName = requestData.optString("username");
            String uPass = requestData.optString("password");
            Integer attemptCount = loginAttempts.get(uName);
            User user = User.getExistingUser(uName);
            if (this.maxPasswordRetries > 0) {
                if (attemptCount != null
                        && attemptCount > this.getMaxPasswordAttempts()) {
                    resetLoginAttempts(uName);
                    worker.setResponseData("maxpassattempts");
                    messageToClient(worker);
                    boolean isLoggedIn = user.getUserProperty("IS_LOGGED_IN")
                            .equals("1");
                    if (!isLoggedIn) {
                        user.setUserProperty("IS_DISABLED", "1", true);
                    }

                }
            }
            boolean loginSuccess = loginUser(user, uPass, worker);
            if (loginSuccess) {
                resetLoginAttempts(uName);
                createSession(worker, user);
            } else {
                if (this.maxPasswordRetries > 0) {
                    Integer attempts = loginAttempts.get(uName);
                    if (attempts == null) {
                        attempts = 1;
                        loginAttempts.put(uName, attempts);
                    }
                    attempts++;
                    loginAttempts.replace(uName, attempts);
                }
            }
        } catch (Exception e) {
            worker.setResponseData("notexist");
            messageToClient(worker);

        }
    }

    private boolean isPasswordExpired(long userTime) {
        int passLife = this.getPassWordLife(); // in minutes
        // if passlife is zero ignore password expiry
        if (passLife != 0) {
            long sysTime = System.currentTimeMillis();
            long diff = sysTime - userTime;
            long minDiff = (diff / 60000);
            if (minDiff > passLife) {
                return true;
            }
            return false;
        }
        return false;
    }

    /**
     * this method authenticates a user before he is allowed access to the
     * system
     *
     * @param userName the user name the user is assigned to allow him or her
     * access the system, this may even be an email address
     * @param pass this is the password the user uses to access the system.
     * @param clientID this is the id of the connecting client
     * @param app this is the name of the application the user is connecting to
     *
     */
    private boolean loginUser(User user, String pass, ClientWorker worker) {
        try {
            String userName = user.getUserProperty("USER_NAME");
            if (user.getUserProperty("IS_DISABLED").equals("1")) {
                worker.setResponseData("disabled");
                messageToClient(worker);
                return false;
            } else if (user.getUserProperty("CHANGE_PASSWORD").equals("1")) {
                worker.setResponseData("changepass");
                messageToClient(worker);
                return false;
            } else if (isPasswordExpired(Long.parseLong(user.getUserProperty("IS_PASSWORD_EXPIRED")))) {
                io.log("User password is expired : " + userName, Level.INFO,Server.class);
                worker.setResponseData("changepass");
                messageToClient(worker);
                return false;
            } else if (!this.getMultipleLoginState()) {
                if (user.getUserProperty("IS_LOGGED_IN").equals("1")) {
                    worker.setResponseData("loggedin");
                    messageToClient(worker);
                    io.log("User already logged in: " + userName, Level.INFO,Server.class);
                    return false;
                }
                return authenticateUser(user, pass, worker);
            }
            return authenticateUser(user, pass, worker);
        } catch (Exception ex) {
            io.log(ex, Level.SEVERE, Server.class);
            worker.setResponseData("error");
            messageToClient(worker);
            return false;
        }
    }

    /**
     * this method authenticates the user given the username, password and
     * client worker
     */
    private boolean authenticateUser(User user, String pass, ClientWorker worker) {
        try {
            // this is a new password
            String userName = user.getUserProperty("USER_NAME");
            String pass_user = Security.toBase64(Security.makePasswordDigest(userName, pass.toCharArray()));
            String pass_stored = user.getUserProperty("PASS_WORD");
            JSONObject object = new JSONObject();
            if (pass_user.equals(pass_stored)) {
                // mark the user as logged in
                Datastore.updateSingleEntity("USERS", "USER_NAME", userName,"IS_LOGGED_IN", "1", FilterOperator.EQUAL);
                object.put("response", "loginsuccess");
                object.put("user", userName);
                object.put("rand", worker.getSession().getId());
                object.put("host", user.getUserProperty("HOST"));
                object.put("privileges", user.getUserPrivileges());
                worker.setResponseData(object);
                messageToClient(worker);
                return true;
            }
            io.log("Invalid user credentials : " + userName, Level.INFO,Server.class);
            worker.setResponseData("invalidpass");
            messageToClient(worker);
            return false;

        } catch (Exception e) {
            return false;
        }
    }

    /**
     * this method logs out a user
     *
     * @param worker the id of the connected client
     * @param userName the username of the user that is being logged out
     */
    public void doLogOut(ClientWorker worker, String userName) {
        logoutUser(worker.getSession());
        sessions.remove(worker.getSession().getId());
        resetTimeout(userName);
        resetLoginAttempts(userName);
    }

    /**
     * the strategy is to send the response directly to the client if this
     * worker has no root worker id. if this worker has a root worker id, it
     * means it was spawned from a root worker, so check if this is the last
     * worker, if it is take all the pending data and send it to the client. the
     * keys for the response are servicename_messagename
     *
     * @param worker the client worker that we are responding to
     *
     */
    public void messageToClient(ClientWorker worker) {
        try {
            String rootWorkerId = worker.getRootWorkerID();
            if (rootWorkerId == null && worker.getPropagateResponse()) {
                // this is a root worker, complete the request
                // propagate the response if we have been requested to
                JSONObject object = new JSONObject();
                object.put("data", worker.getResponseData());
                object.put("reason", worker.getReason());
                worker.toClient(object);
            } else if (rootWorkerId == null) {
                // do nothing because we shouldnt propagate
            } else {
                /*
                 * the strategy is to check which workers have their response
                 * data as null if any worker still has no response data, keep
                 * waiting, otherwise bundle up the response and send it
                 */
                JSONObject data = new JSONObject();
                boolean complete = false;
                ClientWorker[] workers = rootWorkers.get(rootWorkerId);
                for (ClientWorker theWorker : workers) {
                    complete = theWorker.getResponseData() != null;
                    JSONObject object = new JSONObject();
                    object.put("data", theWorker.getResponseData());
                    object.put("reason", theWorker.getReason());
                    data.put(
                            theWorker.getService() + "_"
                            + theWorker.getMessage(), object);
                }
                if (complete && worker.getPropagateResponse()) {
                    worker.toClient(data); // propagate response because we have
                    // been asked to do it
                    rootWorkers.remove(rootWorkerId);
                } else if (complete && !worker.getPropagateResponse()) {
                    rootWorkers.remove(rootWorkerId);
                }
            }
        } catch (JSONException ex) {
            io.log(ex, Level.SEVERE, Server.class);
        }
    }

    public void exceptionToClient(ClientWorker worker) {
        try {
            Throwable obj = (Throwable) worker.getResponseData();
            String rootWorkerId = worker.getRootWorkerID();
            if (rootWorkerId == null && worker.getPropagateResponse()) {
                JSONObject object = new JSONObject();
                object.put("exception", obj);
                object.put("reason", worker.getReason());
                object.put("type", "exception");
                object.put("ex_reason", obj.getMessage());
                worker.toClient(object);
            } else if (rootWorkerId == null) {
                // do nothing
            } else {
                /*
                 * the strategy is to check which workers have their response
                 * data as null if any worker still has no response data, keep
                 * waiting, otherwise bundle up the response and send it
                 */
                JSONObject data = new JSONObject();
                boolean complete = false;
                ClientWorker[] workers = rootWorkers.get(rootWorkerId);
                for (ClientWorker theWorker : workers) {
                    complete = theWorker.getResponseData() != null;
                    JSONObject object = new JSONObject();
                    object.put("exception", obj);
                    object.put("reason", theWorker.getReason());
                    object.put("type", "exception");
                    object.put("ex_reason", obj.getMessage());
                    data.put(theWorker.getService() + "_"+ theWorker.getMessage(), object);
                }
                if (complete && worker.getPropagateResponse()) {
                    worker.toClient(data);
                    rootWorkers.remove(rootWorkerId);
                } else if (complete && !worker.getPropagateResponse()) {
                    rootWorkers.remove(rootWorkerId);
                }
            }
        } catch (JSONException ex) {
            io.log(ex, Level.SEVERE, Server.class);
        }
    }

    /**
     * this method logs out a user using the user's session
     *
     * @param session a user's session
     */
    private void logoutUser(HttpSession session) {
        try {
            String userName = (String) session.getAttribute("username");
            String clientIP = (String) session.getAttribute("clientip");
            String logoutID = (String) session.getAttribute("loginid");
            if (logoutID == null) return;

            Datastore.updateSingleEntity("USERS", "USER_NAME", userName,
                    "IS_LOGGED_IN", "0", FilterOperator.EQUAL);
            String IP = InetAddress.getLocalHost().getHostAddress();
            String host = InetAddress.getLocalHost().getHostName();
            Entity logout = new Entity("LOGOUT");
            logout.setProperty("LOGOUT_ID", logoutID);
            logout.setProperty("USER_NAME", userName);
            logout.setProperty("LOGOUT_TIME", System.currentTimeMillis());
            logout.setProperty("SERVER_IP", IP);
            logout.setProperty("SERVER_HOST", host);
            logout.setProperty("CLIENT_IP", clientIP);
            Datastore.insert(logout);
            session.invalidate();
            io.log("User " + userName + " successfully logged out : "+ userName, Level.INFO, Server.class);
        } catch (UnknownHostException ex) {
            java.util.logging.Logger.getLogger(Server.class.getName()).log(
                    Level.SEVERE, null, ex);
        }
    }

    /**
     * this method is used to get an object that has been proxied, to control
     * access to that object by checking whether a user has access to the
     * specified permanent privilege, access to the returned object is
     * controlled by the proxy class that implements the interface wrapped in
     * the resource object
     *
     * @param obj the object we want to have proxy control to
     * @param res the resource object containing the wrapped interface to be
     * implemented by the dynamic proxy class
     * @param clientID the current client
     * @param priv the privilege we want to control to
     * @return the object whose access is controlled through a proxy
     */
    public Object proxify(Object obj, ClientWorker worker, String priv,String clazz) {
        ClassLoader cl = obj.getClass().getClassLoader();
        return Proxy.newProxyInstance(cl, new Class[]{Serviceable.class},
                new PrivilegeHandler(obj, this, worker, priv, clazz));
    }

    /**
     * this class controls access to a server's privileged services or methods
     * it uses java.lang.reflect.Proxy class for dynamic proxies
     */
    private class PrivilegeHandler implements InvocationHandler, java.io.Serializable {

        private Object obj;
        private String priv;
        private String clazz;
        private ClientWorker worker;

        public PrivilegeHandler(Object obj, Server serv, ClientWorker worker,
                String priv, String clazz) {
            this.obj = obj;
            this.worker = worker;
            this.priv = priv;
            this.clazz = clazz;
        }

        private Object[] getSharedData() {
            String serviceName = worker.getService(); // mark_service
            String message = worker.getMessage(); // all_fields
            String key = message + "_" + serviceName;
            Object[] data = sharedRegistry.get(key);
            if (data == null) return null;
            String methodKey = data[0] + "_" + data[1];
            Method method = serviceRegistry.get(methodKey);
            Object[] shareData = new Object[]{data[0], data[1], method}; // messagename,
            // service
            // instance,method
            return shareData;
        }

        private void trackUsage() {
            //check if we are allowed to track usage
            String usageKey = Server.this.config.getInitParameter("track-usage");
            String usageValue = worker.getRequestData().optString(usageKey);
            if (usageKey != null) {
                //a usage key was provided
                Filter filter = new FilterPredicate(usageKey, FilterOperator.EQUAL, usageValue);
                Entity en = Datastore.getSingleEntity("USAGE_STATS", filter);
                if (en == null) {
                    Entity usage = new Entity("USAGE_STATS");
                    usage.setProperty("USAGE_COUNT", 1);
                    usage.setProperty(usageKey, usageValue);
                    Datastore.insert(usage);
                } else {
                    Long count = (Long) en.getProperty("USAGE_COUNT");
                    count++;
                    en.setProperty("USAGE_COUNT", count);
                    Datastore.insert(en);
                }
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            boolean permContains = false;
            Object[] sharedData;
            String uName = null;
            String privState = Server.services.get(worker.getService()).get(2).toString();
            if (privState.equals("yes")) {
                HttpSession ses = worker.getSession();
                uName = (String) ses.getAttribute("username");
                JSONArray privileges = (JSONArray) ses.getAttribute("privileges");
                String rGroup = this.priv;
                permContains = privileges.toList().contains(rGroup); // privilege
                trackUsage();
            } else {
                uName = "anonymous";
            }

            sharedData = getSharedData();

            if (permContains || privState.equals("no")) {
                try {
                    Method met = serviceRegistry.get(worker.getMessage() + "_" + clazz);
                    if (met != null) { //this is the first attempt
                        io.log("[" + uName + "] Service invoked: " + obj.getClass().getSimpleName() + " Method: " + met.getName(), Level.SEVERE, Server.class);
                        return met.invoke(obj, new Object[]{Server.this, worker});
                    } else { //this is a shared method
                         if (sharedData != null) {
                            Object serviceInstance = runtimeServices.get(sharedData[1].toString());
                            Method sharedMethod = (Method) sharedData[2];
                            io.log(" [" + uName + "] Service invoked: " + serviceInstance.getClass().getSimpleName() + " Shared Method: " + sharedMethod.getName(), Level.INFO, Server.class);
                            return sharedMethod.invoke(serviceInstance, new Object[]{Server.this, worker});
                        } else {
                            worker.setResponseData("The specified message " + worker.getMessage() + ""
                                    + " does not exist for service " + worker.getService());
                            messageToClient(worker);
                        }
                    }
                    // if this fails check to see if there is a service that has shared this method with the currently invoked service
                } catch (Exception e) {
                    if (Server.this.debugmode) {
                        e.getCause().printStackTrace();
                    }
                }
            } else {
                worker.setResponseData("No privileges found for requested service");
                messageToClient(worker);
            }

            return null;
        }
    }

    /**
     * this method can be used to invoke a service within another service
     *
     * @param worker this represents the client request
     * @throws SecurityException
     */
    public void invokeService(ClientWorker worker) {
        processClientRequest(worker);
    }

}
