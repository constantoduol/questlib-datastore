package com.quest.access.useraccess;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.quest.access.common.UniqueRandom;
import com.quest.access.common.datastore.Datastore;
import com.quest.access.crypto.Security;
import com.quest.access.useraccess.verification.Action;
import com.quest.access.useraccess.verification.UserAction;

import java.io.Serializable;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author constant oduol
 * @version 1.0(17/3/12)
 */
/**
 * <p>
 * A user is a person or object that has been assigned access rights to server
 * resources, a user is created by specifying the user name, password, host,
 * server the user belongs to and the privileges assignable to the user. When a
 * user is created he is assigned a unique ten digit user id and the user's
 * privileges are saved in the PRIVILEGES table. A user can also be created as
 * having access to a given service. Once a user is created he can log in and
 * access the resources that are accessible to him
 * </p>
 * <p>
 * A user can be renamed after creation provided there is no existing user with
 * the same user name. Users can also be temporarily deleted i.e their details
 * are moved to the USER_HISTORY table Users can also be disabled to prevent
 * access to server resources.
 * </p>
 * <p>
 * When a user logs in to a server a new session is created to keep track of the
 * user logging out the user destroys the user's session Every user has a
 * temporary privilege associated with them, the system can assign users
 * resources that they don't have access to temporarily through their temporary
 * privileges
 * </p>
 *
 */
public class User  {

    private JSONObject userProperties = new JSONObject();

    private String userName;
    /*
     * this are the  resource groups the user has access to
     */
    private JSONArray priv;

    /**
     * constructs a user object, the user's details are stored in the server's
     * database in the USERS table
     *
     * @param userName the desired userName of the new user
     * @param pass the desired password of the new user, if this is not provided
     * the default password of the server is used
     * @param host the host from which this user is expected to connect from
     * @param server the server in which this user is expected to operate
     * @param priv the permanent privileges that are accessible to this user.
     */
    public User(String userName, String pass, String host, Action action, String... privs) throws UserExistsException {
        this(userName, pass, host, null, action, privs);
    }

    /**
     * constructs a user object, the user's details are stored in the server's
     * database in the USERS table
     *
     * @param userName the desired userName of the new user
     * @param pass the desired password of the new user, if this is not provided
     * the default password of the server is used
     * @param host the host from which this user is expected to connect from
     * @param server the server in which this user is expected to operate
     * @param group the user group that this user is being assigned to
     * @param priv the permanent privileges that are accessible to this user.
     * @throws UserExistsException
     */
    public User(String userName, String pass, String host, String group, Action action, String... privs) throws UserExistsException {
        this.userName = userName;
        this.priv = new JSONArray();
        createUser(userName, pass, host, group, action);
        grantPrivileges(privs);

    }

    public User(String userName, String pass, String host, String group, Action action) throws UserExistsException {
        this.userName = userName;
        this.priv = new JSONArray();
        createUser(userName, pass, host, group, action);
    }

    /**
     * constructs a user object, the user's details are stored in the server's
     * database in the USERS table
     *
     * @param userName the desired userName of the new user
     * @param pass the desired password of the new user, if this is not provided
     * the default password of the server is used
     * @param host the host from which this user is expected to connect from
     * @param server the server in which this user is expected to operate
     * @param group the user group that this user is being assigned to
     * @param service the services this user is being assigned access to
     * @throws UserExistsException
     */
    public User(String userName, String pass, String host, String group, Action action, Service... service) throws UserExistsException {
        String[] privs = new String[service.length];
        for (int x = 0; x < service.length; x++) {
            privs[x] = service[x].getServicePrivilege();
        }
        User user = new User(userName, pass, host, group, action, privs);
    }

    /**
     * constructs a user object, the user's details are stored in the server's
     * database in the USERS table
     *
     * @param userName the desired userName of the new user
     * @param pass the desired password of the new user, if this is not provided
     * the default password of the server is used
     * @param host the host from which this user is expected to connect from
     * @param server the server in which this user is expected to operate
     * @param service the services accessible to this user
     * @throws UserExistsException
     */
    public User(String userName, String pass, String host, UserAction action, Service... service) throws UserExistsException {
        this(userName, pass, host, null, action, service);
    }

    /**
     * privately creates a user object which is used by the method
     * getExistingUser()
     *
     * @param userName the name of the user
     * @param serv the server the user belongs to
     * @see #getExistingUser(java.lang.String, com.quest.access.net.Server)
     */
    private User(String userName, JSONObject data) {
        this.userName = userName;
        this.userProperties = data;
    }

    public JSONObject getUserProperties() {
        return this.userProperties;
    }

    public String getUserProperty(String key) {
        JSONArray props = this.userProperties.optJSONArray(key);
        if (props != null) {
            return props.optString(0);
        }
        return "";
    }

    public void setUserProperty(String key, String value, boolean remote) {
        JSONArray props = this.userProperties.optJSONArray(key);
        if (props != null) {
            props.put(value);
        } else {
            try {
                props = new JSONArray();
                props.put(value);
                this.userProperties.put(key, props);
                if (!remote) {
                    return;
                }
            } catch (JSONException ex) {
                java.util.logging.Logger.getLogger(User.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        // io.out("key: "+key+ " value : "+value);
        Datastore.updateSingleEntity("USERS", "USER_NAME", this.userName, key, value, FilterOperator.EQUAL);
    }

    /**
     * this method is used to modify the password of a user
     *
     * @param newPass the new password the user will use to log in
     */
    public void setPassWord(String newPass) {
        try {
            byte[] bytes = Security.makePasswordDigest(this.userName, newPass.toCharArray());
            String passw = Security.toBase64(bytes);
            this.setUserProperty("PASS_WORD", passw, true);
        } catch (Exception e) {

        }
    }

    /**
     * this method permanently deletes a user and all his records this method
     * cannot be undone and user records cannot be recovered
     *
     * @param userName the name of the user we want to permanently delete
     * @param serv the server the user was created in
     */
    public static void deleteUser(String userName) throws NonExistentUserException {
        User user = User.getExistingUser(userName);
        Datastore.deleteSingleEntity("USERS", "USER_ID", user.getUserProperty("USER_ID"), FilterOperator.EQUAL);
        Datastore.deleteSingleEntity("PRIVILEGES", "USER_ID", user.getUserProperty("USER_ID"), FilterOperator.EQUAL);
    }

    /**
     * this method returns the history of a users actions from the database it
     * gives a view of what a user has been doing, this actions only represent
     * those actions that the system chooses to record. specifying a limit of 0
     * returns all the records, caution should be used when specifying a limit
     * of 0 since it could be slow
     */
    public static JSONObject getActionHistory(String userName, int limit) {
        Filter filter = new FilterPredicate("USER_NAME", FilterOperator.EQUAL, userName);
        Iterable<Entity> entities = null;
        if (limit == 0) {
            entities = Datastore.getMultipleEntities("USER_ACTIONS", "ACTION_TIME", SortDirection.DESCENDING, filter);
        } else if (limit > 0) {
            entities = Datastore.getMultipleEntities("USER_ACTIONS", "ACTION_TIME", SortDirection.DESCENDING, FetchOptions.Builder.withLimit(limit), filter);
        }
        return Datastore.entityToJSON(entities);

    }

    /**
     * this method returns the privileges of a user as stored in the database
     * this method is called when a user logs in in order to determine which
     * privileges the user has, privilege information is stored in the
     * PRIVILEGES table. the privilege information is returned in a hash map
     * containing the RESOURCE GROUP NAMES as the keys of the hash map and an
     * array list containing the users resource names
     *
     * @param userName the username of the user we want to obtain privileges
     * @param serv the server where the user was originally created
     */
    public JSONArray getUserPrivileges() {
        Iterable<Entity> privEntities = Datastore.getMultipleEntities("PRIVILEGES", "USER_ID", this.getUserProperty("USER_ID"), FilterOperator.EQUAL);
        return Datastore.entityToJSON(privEntities).optJSONArray("GROUP_ID");
    }

    public static boolean changePassword(String userName, String oldPass, String newPass) {
        try {
            String old_pass = Security.toBase64(Security.makePasswordDigest(userName, oldPass.toCharArray()));
            Entity en = Datastore.getSingleEntity("USERS", "USER_NAME", userName, FilterOperator.EQUAL);
            String pass_stored = en.getProperty("PASS_WORD").toString();
            if (old_pass.equals(pass_stored)) {
                byte[] bytes = Security.makePasswordDigest(userName, newPass.toCharArray());
                String passw = Security.toBase64(bytes);
                Long time = System.currentTimeMillis();
                Datastore.updateSingleEntity("USERS", "USER_NAME", userName, "PASS_WORD", passw, FilterOperator.EQUAL);
                Datastore.updateSingleEntity("USERS", "USER_NAME", userName, "IS_PASSWORD_EXPIRED", time.toString(), FilterOperator.EQUAL);
                Datastore.updateSingleEntity("USERS", "USER_NAME", userName, "CHANGE_PASSWORD", "0", FilterOperator.EQUAL);
                return true;
            } else {
                return false;
            }

        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * this method returns login details for the specified user , each time a
     * user logs in the login details at that time are inserted in one table row
     * of the LOGIN table specifying a limit of 0 returns all the results,
     * specifying a limit of zero should be used with caution since it could be
     * slow. details return include the login time, login id etc. an arraylist
     * containing hashmaps of the login data is returned the keys in the hash
     * map are
     * <ol>
     * <li>SERVER_IP the ip address of the server machine the user logged in
     * from</li>
     * <li>CLIENT_IP the ip address of the client machine the user logged in
     * from</li>
     * <li>LOGIN_ID the system generated log in id </li>
     * <li>LOGIN_TIME the time the user logged in</li>
     * </ol>
     *
     * @param userName the username of the user we want to retrieve the login
     * details
     * @param serv the server this user was originally created in
     * @param limit the number of rows to be retrieved
     * @return an arraylist containing login details
     */
    public static JSONObject getLoginLog(String userName, int limit) {
        Filter filter = new FilterPredicate("USER_NAME", FilterOperator.EQUAL, userName);
        Iterable<Entity> entities = null;
        if (limit == 0) {
            entities = Datastore.getMultipleEntities("LOGIN", "LOGIN_TIME", SortDirection.DESCENDING, filter);
        } else if (limit > 0) {
            entities = Datastore.getMultipleEntities("LOGIN", "LOGIN_TIME", SortDirection.DESCENDING, FetchOptions.Builder.withLimit(limit), filter);
        }

        return Datastore.entityToJSON(entities);
    }

    /**
     * this method returns the details of a user who has successfully logged out
     * of the system such details include the logout time, logout id etc. an
     * arraylist containing hashmaps of the logout data is returned the keys in
     * the hash map are
     * <ol>
     * <li>SERVER_IP the ip address of the server machine the user logged out
     * from</li>
     * <li>CLIENT_IP the ip address of the client machine the user logged out
     * from</li>
     * <li>LOGOUT_ID the system generated log out id which is the same as the
     * login id</li>
     * <li>LOGOUT_TIME the time the user logged out</li>
     * </ol>
     *
     * @param userName the username of the user we want to retrieve the logout
     * details
     * @param serv the server this user was originally created in
     * @param limit the number of rows to be retrieved
     * @return an arraylist containing logout details
     */
    public static JSONObject getLogoutLog(String userName, int limit) {
        Filter filter = new FilterPredicate("USER_NAME", FilterOperator.EQUAL, userName);
        Iterable<Entity> entities = null;
        if (limit == 0) {
            entities = Datastore.getMultipleEntities("LOGOUT", "LOGOUT_TIME", SortDirection.DESCENDING, filter);
        } else if (limit > 0) {
            entities = Datastore.getMultipleEntities("LOGOUT", "LOGOUT_TIME", SortDirection.DESCENDING, FetchOptions.Builder.withLimit(limit), filter);
        }

        return Datastore.entityToJSON(entities);
    }

    /**
     * this method returns an instance of an existing user without trying to
     * recreate the user, the method gets the details of the user and creates a
     * user object for this user that already exists
     *
     */
    public static User getExistingUser(String userName) throws NonExistentUserException {
        Entity en = Datastore.getSingleEntity("USERS", "USER_NAME", userName, FilterOperator.EQUAL);
        if (en == null) {
            throw new NonExistentUserException();
        }
        return new User(userName, Datastore.entityToJSONArray(en));
    }

    /**
     * this method assigns the specified privileges to the specified user
     *
     * @param userName the user name of the user to be assigned privileges
     * @param serv the server the user was created in
     * @param priv the privileges to be assigned
     */
    public void grantPrivileges(String... privs) {
        for (String privilege : privs) {
            Filter filter = new FilterPredicate("USER_ID", FilterOperator.EQUAL, this.getUserProperty("USER_ID"));
            Filter filter1 = new FilterPredicate("GROUP_ID", FilterOperator.EQUAL, privilege);
            Entity en = Datastore.getSingleEntity("PRIVILEGES", filter, filter1);
            if (en == null) {
                Entity priv = new Entity("PRIVILEGES");
                priv.setProperty("USER_ID", this.getUserProperty("USER_ID"));
                priv.setProperty("GROUP_ID", privilege);
                Datastore.insert(priv);
            }
        }
    }

    /**
     * this method revokes the specified privileges from the specified users
     *
     * @param userName the user name of the user to be revoked privileges
     * @param serv the server the user was created in
     * @param priv the privileges to be revoked
     */
    public void revokePrivileges(String... privs) throws NonExistentUserException {
        try {
            for (String name : privs) {
                if (name == null) {
                    // this means no such privilege exists
                    continue;
                }
                Filter filter = new FilterPredicate("USER_ID", FilterOperator.EQUAL, this.getUserProperty("USER_ID"));
                Filter filter1 = new FilterPredicate("GROUP_ID", FilterOperator.EQUAL, name);
                Datastore.deleteSingleEntity("PRIVILEGES", filter, filter1);
            }
        } catch (Exception e) {
            throw new NonExistentUserException();
        }
    }

    /**
     * returns a string representation of a user
     */
    @Override
    public String toString() {
        return "User[" + this.userName + " : " + this.getUserProperty("USER_ID") + "]";
    }

    /*----------------private implementation---------------------*/
    private void createUser(String userName, String pass, String host, String group, Action action) throws UserExistsException {
        group = group == null ? "unassigned" : group;
        // check to ensure the user_id is always unique
        Entity en = Datastore.getSingleEntity("USERS", "USER_NAME", userName, FilterOperator.EQUAL);
        if (en != null) {
            throw new UserExistsException();
        }
        UniqueRandom ur = new UniqueRandom(20);
        String nextRandom = ur.nextRandom();
        try {
            byte[] bytes = Security.makePasswordDigest(userName, pass.toCharArray());
            String passw = Security.toBase64(bytes);
            Long time = System.currentTimeMillis();
            Entity user = new Entity("USERS");
            user.setProperty("USER_ID", nextRandom);
            user.setProperty("USER_NAME", userName);
            user.setProperty("PASS_WORD", passw);
            user.setProperty("HOST", host);
            user.setProperty("LAST_LOGIN", time);
            user.setProperty("IS_LOGGED_IN", "0");
            user.setProperty("IS_DISABLED", "0");
            user.setProperty("IS_PASSWORD_EXPIRED", time);
            user.setProperty("CHANGE_PASSWORD", "0");
            user.setProperty("CREATED", time);
            user.setProperty("GROUPS", group);
            user.setProperty("ACTION_ID", action.getActionID());
            Datastore.insert(user);
            action.saveAction();
            this.userProperties = Datastore.entityToJSONArray(user);
        } catch (Exception e) {

        }

    }

}
