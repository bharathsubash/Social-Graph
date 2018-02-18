package utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.bson.BsonUndefined;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DBHelper {
	
	public static HashMap<String, Serializable> buildMap(DBObject object) {
		Iterator<String> iterator = object.keySet().iterator();
		HashMap<String, Serializable> map = new HashMap<String, Serializable>();
		while (iterator.hasNext()) {
			String key = iterator.next();
			if (object.get(key) instanceof BasicDBObject) {
				map.put(key, buildMap((DBObject) object.get(key)));
			} else if (object.get(key) instanceof BasicDBList) {
				ArrayList<Serializable> dbList = (ArrayList<Serializable>) object.get(key);
				Iterator<Serializable> listIterator = dbList.iterator();
				ArrayList<Serializable> newList = new ArrayList<Serializable>();
				while (listIterator.hasNext()) {
					Serializable listObject = listIterator.next();
					if (listObject instanceof DBObject) {
						newList.add(buildMap((DBObject) listObject));
					} else {
						newList.add(listObject);
					}
				}
				map.put(key, newList);
			}  else if(object.get(key) instanceof BsonUndefined) {
				// if value is bsonundefined it can be ignored 
			}  else {
				map.put(key, (Serializable) object.get(key));
			}
		}
		return map;
	}

}
