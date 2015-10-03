package com.quest.access.common.datastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.PropertyProjection;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.CompositeFilterOperator;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.quest.access.common.io;

/**
 *
 * @author constant oduol
 * @version 1.0(2/7/2014)
 */
//update,select, insert,delete
public class Datastore {

    private static DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();

    public static void insert(String entityName, String primaryValue, HashMap<String, Object> values) {
        Key key = KeyFactory.createKey(entityName, primaryValue);
        Entity entity = new Entity(entityName, key);
        Iterator iter = values.keySet().iterator();
        while (iter.hasNext()) {
            String propertyName = (String) iter.next();
            Object propertValue = values.get(propertyName);
            entity.setProperty(propertyName, propertValue);
        }
        datastore.put(entity);
    }

    public static void insert(Entity en) {
        datastore.put(en);
    }

    public static void insert(String entityName, String[] props, Object[] values) {
        Entity entity = new Entity(entityName);
        for (int x = 0; x < props.length; x++) {
            entity.setProperty(props[x], values[x]);
        }
        datastore.put(entity);
    }

    public static void insert(String entityName, String primaryValue, String[] props, Object[] values) {
        Key key = KeyFactory.createKey(entityName, primaryValue);
        Entity entity = new Entity(entityName, key);
        for (int x = 0; x < props.length; x++) {
            entity.setProperty(props[x], values[x]);
        }
        datastore.put(entity);
    }

    public static void insert(String entityName, Key key, String[] props, Object[] values) {
        Entity entity = new Entity(entityName, key);
        for (int x = 0; x < props.length; x++) {
            entity.setProperty(props[x], values[x]);
        }
        datastore.put(entity);
    }

    public static void insert(String entityName, String primaryValue, JSONObject values) {
        Key key = KeyFactory.createKey(entityName, primaryValue);
        Entity entity = new Entity(entityName, key);
        Iterator iter = values.keys();
        try {
            while (iter.hasNext()) {
                String propertyName = (String) iter.next();
                Object propertValue = values.get(propertyName);
                entity.setProperty(propertyName, propertValue);
            }
        } catch (Exception e) {

        }
        datastore.put(entity);
    }

    public static void insert(String entityName, HashMap<String, Object> values) {
        Entity entity = new Entity(entityName);
        Iterator iter = values.keySet().iterator();
        while (iter.hasNext()) {
            String propertyName = (String) iter.next();
            Object propertValue = values.get(propertyName);
            entity.setProperty(propertyName, propertValue);
        }
        datastore.put(entity);
    }

    public static void insert(String entityName, JSONObject values) {
        Entity entity = new Entity(entityName);
        Iterator iter = values.keys();
        try {
            while (iter.hasNext()) {
                String propertyName = (String) iter.next();
                Object propertValue = values.get(propertyName);
                entity.setProperty(propertyName, propertValue);
            }
        } catch (Exception e) {

        }
        datastore.put(entity);
    }

    public static Entity getSingleEntity(String entityName, String propertyName, Object value, FilterOperator filterOperator) {
        Filter filter = new FilterPredicate(propertyName, filterOperator, value);
        Query query = new Query(entityName).setFilter(filter);
        PreparedQuery pq = datastore.prepare(query);
        return pq.asSingleEntity();
    }

    public static Entity getSingleEntity(String entityName, Filter... filters) {
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        PreparedQuery pq = datastore.prepare(query);
        return pq.asSingleEntity();
    }

    public static Iterable<Entity> getMultipleEntities(String entityName, Filter... filters) {
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        PreparedQuery pq = datastore.prepare(query);
        return pq.asIterable();
    }
    
    public static Iterable<Entity> getMultipleEntities(String entityName,PropertyProjection[] projs, Filter... filters) {
        Query query = new Query(entityName);
        for(PropertyProjection proj : projs){
            if(proj.getName().equals("*")) continue;
            query.addProjection(proj);
        }
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        PreparedQuery pq = datastore.prepare(query);
        return pq.asIterable();
    }

    public static Iterable<Entity> getMultipleEntities(String entityName, Key key, Filter... filters) {
        Query query = new Query(entityName);
        query.setAncestor(key);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        PreparedQuery pq = datastore.prepare(query);
        return pq.asIterable();
    }

    public static List<Entity> getMultipleEntitiesAsList(String entityName, Filter... filters) {
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        PreparedQuery pq = datastore.prepare(query);
        return pq.asList(FetchOptions.Builder.withDefaults());
    }

    public static List<Entity> getMultipleEntitiesAsList(String entityName, String propertyName, String value, FilterOperator filterOperator) {
        Filter filter = new FilterPredicate(propertyName, filterOperator, value);
        Query query = new Query(entityName).setFilter(filter);
        PreparedQuery pq = datastore.prepare(query);
        return pq.asList(FetchOptions.Builder.withDefaults());
    }

    public static Iterable<Entity> getMultipleEntities(String entityName, String sortProperty, SortDirection direction, Filter... filters) {
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        query.addSort(sortProperty, direction);
        PreparedQuery pq = datastore.prepare(query);
        return pq.asIterable();
    }

    public static List<Entity> getMultipleEntitiesAsList(String entityName, String sortProperty, SortDirection direction, Filter... filters) {
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        query.addSort(sortProperty, direction);
        PreparedQuery pq = datastore.prepare(query);
        return pq.asList(FetchOptions.Builder.withDefaults());
    }

    public static Iterable<Entity> getMultipleEntities(String entityName, String sortProperty, SortDirection direction, FetchOptions options, Filter... filters) {
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        query.addSort(sortProperty, direction);
        PreparedQuery pq = datastore.prepare(query);
        return pq.asIterable(options);
    }
    
    public static Iterable<Entity> getMultipleEntities(String entityName, FetchOptions options, Filter... filters) {
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        PreparedQuery pq = datastore.prepare(query);
        return pq.asIterable(options);
    }

    public static Iterable<Entity> getMultipleEntities(String entityName, String propertyName, String value, FilterOperator filterOperator) {
        Filter filter = new FilterPredicate(propertyName, filterOperator, value);
        Query query = new Query(entityName).setFilter(filter);
        PreparedQuery pq = datastore.prepare(query);
        return pq.asIterable();
    }
    
    public static Iterable<Entity> getEntities(Query q){
        PreparedQuery pq = datastore.prepare(q);
        return pq.asIterable();
    }

    public static JSONObject entityToJSON(Entity en) {
        JSONObject obj = new JSONObject();
        try {
            Map map = en.getProperties();
            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String propertyName = iter.next();
                Object value = map.get(propertyName);
                obj.put(propertyName, value);
            }
        } catch (Exception e) {

        }
        return obj;
    }

    public static JSONObject entityToJSONArray(Entity en) {
        JSONObject obj = new JSONObject();
        try {
            Map map = en.getProperties();
            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String propertyName = iter.next();
                Object value = map.get(propertyName);
                obj.put(propertyName, new JSONArray().put(value));
            }
        } catch (Exception e) {

        }
        return obj;
    }

    public static JSONObject entityToJSON(Iterable<Entity> iterable) {
        JSONObject obj = new JSONObject();
        try {
            for (Entity en : iterable) {
                Map map = en.getProperties();
                Iterator<String> iter = map.keySet().iterator();
                while (iter.hasNext()) {
                    String propertyName = iter.next();
                    Object value = map.get(propertyName);
                    if (obj.opt(propertyName) == null) {
                        obj.put(propertyName, new JSONArray().put(value));
                    } else {
                        ((JSONArray) obj.opt(propertyName)).put(value);
                    }
                }
            }
        } catch (Exception e) {

        }
        return obj;
    }

    public static Iterable<Entity> getAllEntities(String entityName, FetchOptions options) {
        Query query = new Query(entityName);
        PreparedQuery pq = datastore.prepare(query);
        return pq.asIterable(options);
    }

    public static Iterable<Entity> getAllEntities(String entityName) {
        Query query = new Query(entityName);
        PreparedQuery pq = datastore.prepare(query);
        return pq.asIterable();
    }
    
    public static Iterable<Entity> getAllEntities(String entityName,String sortProperty,SortDirection direction) {
        Query query = new Query(entityName);
        query.addSort(sortProperty, direction);
        PreparedQuery pq = datastore.prepare(query);
        return pq.asIterable();
    }

    public static List<Entity> getAllEntitiesAsList(String entityName) {
        Query query = new Query(entityName);
        PreparedQuery pq = datastore.prepare(query);
        return pq.asList(FetchOptions.Builder.withDefaults());
    }

    public static void updateSingleEntity(String entityName, String primaryKey, String primaryKeyValue, String propertyName, String propertyValue, FilterOperator filterOperator) {
        Filter filter = new FilterPredicate(primaryKey, filterOperator, primaryKeyValue);
        Query query = new Query(entityName).setFilter(filter);
        PreparedQuery pq = datastore.prepare(query);
        Entity en = pq.asSingleEntity();
        en.setProperty(propertyName, propertyValue);
        datastore.put(en);
    }

    public static void updateSingleEntity(String entityName, String primaryKey, String primaryKeyValue, String[] propertyNames, Object[] propertyValues, FilterOperator filterOperator) {
        Filter filter = new FilterPredicate(primaryKey, filterOperator, primaryKeyValue);
        Query query = new Query(entityName).setFilter(filter);
        PreparedQuery pq = datastore.prepare(query);
        Entity en = pq.asSingleEntity();
        for (int x = 0; x < propertyNames.length; x++) {
            en.setProperty(propertyNames[x], propertyValues[x]);
        }
        datastore.put(en);
    }

    public static void updateSingleEntity(String entityName, String[] propertyNames, Object[] propertyValues, Filter... filters) {
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        PreparedQuery pq = datastore.prepare(query);
        Entity en = pq.asSingleEntity();
        for (int x = 0; x < propertyNames.length; x++) {
            en.setProperty(propertyNames[x], propertyValues[x]);
        }
        datastore.put(en);
    }
    
    public static void updateMultipeEntities(String entityName, String[] propertyNames, Object[] propertyValues, Filter... filters) {
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        PreparedQuery pq = datastore.prepare(query);
        Iterable<Entity> iter = pq.asIterable();
        for(Entity en : iter){
            for (int x = 0; x < propertyNames.length; x++) {
                en.setProperty(propertyNames[x], propertyValues[x]);
            }
            datastore.put(en);
        }
    }

    public static void updateMultipeEntities(String entityName, String primaryKey, String primaryKeyValue, String propertyName, Object propertyValue, FilterOperator filterOperator) {
        Filter filter = new FilterPredicate(primaryKey, filterOperator, primaryKeyValue);
        Query query = new Query(entityName).setFilter(filter);
        PreparedQuery pq = datastore.prepare(query);
        for (Entity en : pq.asIterable()) {
            en.setProperty(propertyName, propertyValue);
            datastore.put(en);
        }
    }

    public static void updateAllEntities(String entityName, String propertyName, Object newValue) {
        Query query = new Query(entityName);
        PreparedQuery pq = datastore.prepare(query);
        for (Entity en : pq.asIterable()) {
            en.setProperty(propertyName, newValue);
            datastore.put(en);
        }
    }

    public static void deleteSingleEntity(String entityName, String propertyName, Object value, FilterOperator filterOperator) {
        Filter filter = new FilterPredicate(propertyName, filterOperator, value);
        Query query = new Query(entityName).setFilter(filter);
        PreparedQuery pq = datastore.prepare(query);
        Entity en = pq.asSingleEntity();
        if (en != null) {
            datastore.delete(en.getKey());
        }
    }

    public static void deleteSingleEntity(String entityName, Filter... filters) {
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        PreparedQuery pq = datastore.prepare(query);
        Entity en = pq.asSingleEntity();
        if (en != null) {
            datastore.delete(en.getKey());
        }
    }
    
    public static void deleteMultipleEntities(String entityName,Filter...filters){
        Query query = new Query(entityName);
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        PreparedQuery pq = datastore.prepare(query);
        for(Entity en : pq.asIterable()){
            datastore.delete(en.getKey());
        }
    }

    public static void deleteMultipleEntities(String entityName, String propertyName, Object value, FilterOperator filterOperator) {
        Filter filter = new FilterPredicate(propertyName, filterOperator, value);
        Query query = new Query(entityName).setFilter(filter);
        PreparedQuery pq = datastore.prepare(query);
        for (Entity en : pq.asIterable()) {
            datastore.delete(en.getKey());
        }
    }

    public static void deleteAllEntities(String entityName) {
        Query query = new Query(entityName);
        PreparedQuery pq = datastore.prepare(query);
        for (Entity en : pq.asIterable()) {
            datastore.delete(en.getKey());
        }
    }
    
    public static Long getCount(String entity,Filter ... filters){
        Query query = new Query("__Stat_"+entity+"__");
        if (filters.length == 0) {

        } else if (filters.length == 1) {
            query.setFilter(filters[0]);
        } else {
            query.setFilter(CompositeFilterOperator.and(filters));
        }
        Entity entityStat = datastore.prepare(query).asSingleEntity();
        return (Long) entityStat.getProperty("count");  
    }
    
    

    public static FilterOperator getFilterOperator(String operator) {
        FilterOperator op = null;
        if (operator.equals("=")) {
            op = FilterOperator.EQUAL;
        } else if (operator.equals(">")) {
            op = FilterOperator.GREATER_THAN;
        } else if (operator.equals(">=")) {
            op = FilterOperator.GREATER_THAN_OR_EQUAL;
        } else if (operator.equals("<")) {
            op = FilterOperator.LESS_THAN;
        } else if (operator.equals("<=")) {
            op = FilterOperator.LESS_THAN_OR_EQUAL;
        } else if (operator.equals("!=")) {
            op = FilterOperator.NOT_EQUAL;
        }
        return op;
    }
//
//    public static List<Entity> joinQuery(String[] entityNames, String[] joinProps, String[] sortProps, SortDirection[] dirs, Filter[][] filters) {
//	   //fetch the entities we wish to join on
//        //STOCK_DATA, USER_ACTIONS, PRODUCT_DATA, STOCK_DATA
//        //ID, ACTION_ID, ID, PRODUCT_ID
//        //join recursively
//        //we can only compare two properties at a time
//        ArrayList<Entity> list = new ArrayList<>();
//        HashMap<String, List<Entity>> allEntities = new HashMap<>();
//
//        for (int x = 0; x < entityNames.length; x++) {
//            //do not fetch from datastore if we already have the entity
//            List<Entity> entities;
//            if (sortProps != null && sortProps[x] != null) {
//                entities = Datastore.getMultipleEntitiesAsList(entityNames[x], sortProps[x], dirs[x], filters[x]);
//            } else {
//                entities = Datastore.getMultipleEntitiesAsList(entityNames[x], filters[x]);
//            }
//
//            allEntities.put(entityNames[x], entities);
//        }
//
//	   //at this point we have all the data
//        //we can only compare two entities at a time
//        int length = joinProps.length / 2;
//
//        for (int x = 0; x < length; x = x + 2) {
//            for (int y = x; y < length; y = y + 2) {
//                String entityName = entityNames[y];
//                String entityName1 = entityNames[y + 1];
//
//                List<Entity> entitiesOne = allEntities.get(entityName);
//                List<Entity> entitiesTwo = allEntities.get(entityName1);
//
//		       //compare two entities on given properties at a time
//                //PRODUCT_DATA and STOCK_DATA on ID and PRODUCT_ID
//                //then USER_ACTIONS and STOCK_DATA on ACTION_ID and ID
//                //go through the largest list comparing on the joinProperties
//                ArrayList<Object> keys = new ArrayList<>(); //keys and values for the longest
//                ArrayList<Entity> values = new ArrayList<>();
//                HashMap<Object, Entity> map = new HashMap<>();
//                for (int z = 0; z < entitiesOne.size(); z++) {
//                    Entity en = entitiesOne.get(z);
//                    keys.add(en.getProperty(joinProps[y])); //allow duplicates
//                    values.add(en);
//                }
//
//                for (int m = 0; m < entitiesTwo.size(); m++) {
//                    Entity en = entitiesTwo.get(m);
//                    map.put(en.getProperty(joinProps[y + 1]), en);
//                }
//
//                for (int i = 0; i < keys.size(); i++) {
//                    Object key = keys.get(i);
//                    Entity en = values.get(i);
//                    Entity en1 = map.get(key);
//                    if (en != null && en1 != null) {
//                        Iterator<String> iter = en1.getProperties().keySet().iterator();
//                        while (iter.hasNext()) {
//                            String prop = iter.next();
//                            if (en.getProperty(prop) == null) { //if this property does not exist
//                                en.setProperty(prop, en1.getProperty(prop));
//                            }
//                        }
//
//                    }
//
//                    list.add(en);
//                }
//
//                //join the maps
//            }
//            if (length < joinProps.length) {
//                length = length * 2;
//            }
//        }
//        
//        return list;
//    }
//    
    
    //this is a two way join
    public static List<Entity> twoWayJoin(String [] entityNames, String[] joinProps, String [] sortProps, SortDirection [] dirs,Filter[] filters1,Filter[] filters2 ){
        List<Entity> joined = new ArrayList();
        List<Entity> entitiesOne = sortProps != null && sortProps[0] != null ? 
                Datastore.getMultipleEntitiesAsList(entityNames[0], sortProps[0], dirs[0], filters1) :
                Datastore.getMultipleEntitiesAsList(entityNames[0], filters1);
        
        List<Entity> entitiesTwo = sortProps != null && sortProps[1] != null ? 
                Datastore.getMultipleEntitiesAsList(entityNames[1], sortProps[1], dirs[1], filters2) :
                Datastore.getMultipleEntitiesAsList(entityNames[1], filters2);
        // List max = entitiesOne.size() > entitiesTwo.size() ? entitiesOne : entitiesTwo;
        // List min = entitiesOne.size() < entitiesTwo.size() ? entitiesOne : entitiesTwo;
        //no of comparisons made = maxLength * minLength; for a basic nested loop join
        for (Entity en1 : entitiesOne) {
            for (Entity en2 : entitiesTwo) {
               if( en1.getProperty(joinProps[0]).equals(en2.getProperty(joinProps[1])) ){
                   en1.setPropertiesFrom(en2); //copy properties of en2
                   joined.add(en1);
               }
            }
        }
        //join the two entities one and two
        //strategy is to iterate on the larger one while doing the join
        //[Entity1, Entity2, Entity3, Entity4, Entity5]
        //[Entity6, Entity7, Entity8]
        //
        return joined;
    }
    
    
   
    //we deal with the two way one and then join with any further result
    //String[] entityNames, String[] joinProps, String[] sortProps, SortDirection[] dirs, Filter[][] filters
    public static ArrayList<Entity> multiJoin(String[] entityNames, String[] joinProps, String[] sortProps, SortDirection[] dirs, Filter[][] filters){
      //we join two entities at a time
        //if a crazy guy provides less than 2 entity names, shout at him...
        if(entityNames.length < 2) throw new RuntimeException("Insufficient entities for multi join operation");
        //perform a join operation on the first two operands and join the results 
        
        //TABLE_ONE,TABLE_TWO               entitynames
        //ID, ID                            join props
        //PRODUCT_NAME,SUPPLIER_NAME        sort props
        //ASC,ASC                           sort dirs        
        
        
        //TABLE_ONE,TABLE_TWO,TABLE_THREE, TABLE_FOUR       entitynames
        //ID, ID, ID, ID                                    join props
        //PRODUCT_NAME,SUPPLIER_NAME,CREATED,CREATED        sort props
        //ASC,ASC,ASC,ASC                                   sort dirs 
        ArrayList<Entity> multiJoin = new ArrayList<>();
        for(int x = 0; x < entityNames.length; x++){
           String [] jProps = new String[]{joinProps[x],joinProps[x+1]};
           String [] sProps = new String[]{sortProps[x],sortProps[x+1]};
           SortDirection [] dir = new SortDirection[]{dirs[x],dirs[x+1]};
           List<Entity> twoWayJoin = twoWayJoin(new String[]{entityNames[x],entityNames[x + 1]},jProps, sProps, dir, filters[x], filters[x+1]); 
           for(int y = 0; y < twoWayJoin.size(); y++){
               for(int z = 0; z < multiJoin.size(); z++){
                   Entity en = twoWayJoin.get(y);
                   Entity en1 = twoWayJoin.get(z);
                   en.setPropertiesFrom(en1);
                   multiJoin.set(z, en);
               }
           }
        }
        return multiJoin;
       
    }

   //we compare two columns/properties on a single entity
    //fetch the 
    public static List<Entity> compareQuery(String entityName, String propertyOne, String operator, String propertyTwo, Filter... filters) {
        List<Entity> entities = Datastore.getMultipleEntitiesAsList(entityName,filters);
        ArrayList list = new ArrayList();
        for (Entity en : entities) {
            Object one = en.getProperty(propertyOne);
            Object two = en.getProperty(propertyTwo);
            // we can compare
            try {
                // cast as a double to first if fail cast as object
                Double first = Double.parseDouble(one.toString());
                Double second = Double.parseDouble(two.toString());
                if (operator.equals("=")) {
                    if (first == second) {
                        list.add(en);
                    }
                } else if (operator.equals("<")) {
                    if (first < second) {
                        list.add(en);
                    }
                } else if (operator.equals("<=")) {
                    if (first <= second) {
                        list.add(en);
                    }
                } else if (operator.equals(">")) {
                    if (first > second) {
                        list.add(en);
                    }
                } else if (operator.equals(">=")) {
                    if (first >= second) {
                        list.add(en);
                    }
                } else if (operator.equals("!=")) {
                    if (first != second) {
                        list.add(en);
                    }
                }

            } catch (Exception e) {
                // these are other objects apart from numbers
                if (operator.equals("=")) {
                    if (one.equals(two)) {
                        list.add(en);
                    }
                } else if (operator.equals("!=")) {
                    if (!one.equals(two)) {
                        list.add(en);
                    }
                }
                // ignore other operators because they dont apply
            }

        }
        return list;

    }

}
