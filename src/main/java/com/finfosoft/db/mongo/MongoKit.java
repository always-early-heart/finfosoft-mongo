/**
 * Copyright (c) 2011-2013, kidzhou 周磊 (zhouleib1412@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.finfosoft.db.mongo;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;

import com.jfinal.log.Logger;
import com.jfinal.plugin.activerecord.Page;
import com.jfinal.plugin.activerecord.Record;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoKit {

    protected static Logger logger = Logger.getLogger(MongoKit.class);

    private static MongoClient client;
    private static DB defaultDb;

    public static void init(MongoClient client, String database) {
        MongoKit.client = client;
        MongoKit.defaultDb = client.getDB(database);

    }

    public static void updateFirst(String collectionName, Map<String, Object> q, Map<String, Object> o) {
        MongoKit.getCollection(collectionName).findAndModify(toDBObject(q), toDBObject(o));
    }
    
    public static void updateFirst(String collectionName, Map<String, Object> q, Record o) {
        MongoKit.getCollection(collectionName).findAndModify(toDBObject(q), toDbObject(o));
    }
    
    public static int getUniqueId(String idName) {
    	BasicDBObject query=new BasicDBObject().append("name", idName);
    	BasicDBObject update=new BasicDBObject().append("$inc", 
    			new BasicDBObject().append("id", 1));
    	BasicDBObject ids=(BasicDBObject)MongoKit.getCollection("ids").findAndModify(query, update);
    	int id=ids.getInt("id");
    	return id;
    }

    public static int removeAll(String collectionName) {
        return MongoKit.getCollection(collectionName).remove(new BasicDBObject()).getN();
    }

    public static int remove(String collectionName, Map<String, Object> filter) {
        return MongoKit.getCollection(collectionName).remove(toDBObject(filter)).getN();
    }

    public static int save(String collectionName, List<Record> records) {
        List<DBObject> objs = new ArrayList<DBObject>();
        for (Record record : records) {
            objs.add(toDbObject(record));
        }
        return MongoKit.getCollection(collectionName).insert(objs).getN();

    }

    public static int save(String collectionName, Record record) {
        return MongoKit.getCollection(collectionName).save(toDbObject(record)).getN();
    }
    
    public static int save(String collectionName, Map<String,Object> record) {
        return MongoKit.getCollection(collectionName).save(toDBObject(record)).getN();
    }
    
    public static int save(String collectionName, BasicDBObject record) {
        return MongoKit.getCollection(collectionName).save(record).getN();
    }

    public static Record findFirst(String collectionName) {
        return toRecord(MongoKit.getCollection(collectionName).findOne());
    }
    
    public static Record findById(String collectionName,String id) {
    	BasicDBObject conditons = new BasicDBObject();
    	conditons.put("_id", new ObjectId(id));
        return toRecord(MongoKit.getCollection(collectionName).findOne(conditons));
    }
    
    public static Record findById(String collectionName,int id) {
    	BasicDBObject conditons = new BasicDBObject();
    	conditons.put("_id", id);
        return toRecord(MongoKit.getCollection(collectionName).findOne(conditons));
    }
    
    public static Record findById(String collectionName,ObjectId _id) {
    	BasicDBObject conditons = new BasicDBObject();
    	conditons.put("_id", _id);
        return toRecord(MongoKit.getCollection(collectionName).findOne(conditons));
    }

    public static Page<Record> paginate(String collection, int pageNumber, int pageSize) {
        return paginate(collection, pageNumber, pageSize, null, null, null, null);
    }

    public static Page<Record> paginate(String collection, int pageNumber, int pageSize, Map<String, Object> filter) {
        return paginate(collection, pageNumber, pageSize, filter, null, null, null);
    }

    public static Page<Record> paginate(String collection, int pageNumber, int pageSize, Map<String, Object> filter,
            Map<String, Object> like) {
        return paginate(collection, pageNumber, pageSize, filter, like, null, null);
    }
    
    public static Page<Record> paginate(String collection, int pageNumber, int pageSize, Map<String, Object> filter,
            Map<String, Object> like, Map<String, Object> sort) {
        return paginate(collection, pageNumber, pageSize, filter, like, sort, null);
    }

    public static Page<Record> paginate(String collection, int pageNumber, int pageSize, Map<String, Object> filter,
            Map<String, Object> like, Map<String, Object> sort, Map<String, Object> regex) {
        DBCollection logs = MongoKit.getCollection(collection);
        BasicDBObject conditons = new BasicDBObject();
        buildFilter(filter, conditons);
        buildLike(like, conditons);
        buildRegex(regex,conditons);
        DBCursor dbCursor = logs.find(conditons);
        page(pageNumber, pageSize, dbCursor);
        sort(sort, dbCursor);
        List<Record> records = new ArrayList<Record>();
        while (dbCursor.hasNext()) {
            records.add(toRecord(dbCursor.next()));
        }
        int totalRow = dbCursor.count();
        if (totalRow <= 0) {
            return new Page<Record>(new ArrayList<Record>(0), pageNumber, pageSize, 0, 0);
        }
        int totalPage = totalRow / pageSize;
        if (totalRow % pageSize != 0) {
            totalPage++;
        }
        Page<Record> page = new Page<Record>(records, pageNumber, pageSize, totalPage, totalRow);
        return page;
    }

    private static void page(int pageNumber, int pageSize, DBCursor dbCursor) {
        dbCursor = dbCursor.skip((pageNumber - 1) * pageSize).limit(pageSize);
    }
    
    
    public static List<Record> query(String collection, Map<String, Object> filter) {
        return query(collection, filter, null, null, null);
    }
    
    public static List<Record> query(String collection, Map<String, Object> filter, 
    		Map<String, Object> like) {
        return query(collection, filter, like, null, null);
    }
    
    public static List<Record> query(String collection, Map<String, Object> filter, 
    		Map<String, Object> like, Map<String, Object> sort) {
        return query(collection, filter, like, sort, null);
    }
    
    public static List<Record> query(String collection, Map<String, Object> filter,
    			Map<String, Object> like, Map<String, Object> sort, Map<String, Object> regex) {
        DBCollection logs = MongoKit.getCollection(collection);
        BasicDBObject conditons = new BasicDBObject();
        buildFilter(filter, conditons);
        buildLike(like, conditons);
        buildRegex(regex, conditons);
        DBCursor dbCursor = logs.find(conditons);
        sort(sort, dbCursor);
        List<Record> records = new ArrayList<Record>();
        while (dbCursor.hasNext()) {
            records.add(toRecord(dbCursor.next()));
        }
        int totalRow = dbCursor.count();
        return records;
    }
    
    /**
     * 刘鹏飞 新增 可进行正则排序的方法
     * @param collection
     * @param conditons
     * @param sort
     * @param regex
     * @param limit
     * @return
     */
    public static List<Record> query(String collection, BasicDBObject conditons, BasicDBObject sort,
			BasicDBObject regex, int limit) {
		DBCollection logs = MongoKit.getCollection(collection);
		if (regex != null) {
			Set<Entry<String, Object>> entrySet = regex.entrySet();
			for (Entry<String, Object> entry : entrySet) {
				String key = entry.getKey();
				Object val = entry.getValue();
				conditons.put(key, MongoKit.getRegexStr(val));
			}
		}
		DBCursor dbCursor = logs.find(conditons);
		if (sort != null) {
			dbCursor.sort(sort);
		}
		if (limit != -1) {
			dbCursor.limit(limit);
		}
		List<Record> records = new ArrayList<Record>();
		while (dbCursor.hasNext()) {
			Record record = new Record();
			record.setColumns(dbCursor.next().toMap());
			records.add(record);
		}
		return records;
	}
    
    public static List<Record> query(String collection, Map<String, Object> filter,Map<String, Object> like, Map<String, Object> sort, Map<String, Object> regex, int limit,BasicDBObject projection) {
    DBCollection logs = MongoKit.getCollection(collection);
    BasicDBObject conditons = new BasicDBObject();
    buildFilter(filter, conditons);
    buildLike(like, conditons);
    buildRegex(regex, conditons);
    DBCursor dbCursor;
    if(projection!=null){
    		dbCursor = logs.find(conditons,projection);
    }else{
    		dbCursor = logs.find(conditons);
    }
    sort(sort, dbCursor);
    if (limit != -1) {
		dbCursor.limit(limit);
	}
    List<Record> records = new ArrayList<Record>();
    while (dbCursor.hasNext()) {
        records.add(toRecord(dbCursor.next()));
    }
    int totalRow = dbCursor.count();
    return records;
}
    
	public static List<Record> query(String collection,
			BasicDBObject conditons, Map<String, Object> sort) {
		DBCollection logs = MongoKit.getCollection(collection);
		DBCursor dbCursor = logs.find(conditons);
		sort(sort, dbCursor);
		List<Record> records = new ArrayList<Record>();
		while (dbCursor.hasNext()) {
			records.add(toRecord(dbCursor.next()));
		}
		return records;
	}
	
	public static List<Record> query(String collection,
			BasicDBObject conditons, BasicDBObject sort ) {
		List<Record> records=query(collection, conditons, sort, -1 );
		return records;
	}
	
	public static List<Record> query(String collection,
			BasicDBObject conditons, BasicDBObject sort, int limit ) {
		DBCollection logs = MongoKit.getCollection(collection);
		DBCursor dbCursor = logs.find(conditons);
		if(sort!=null){
			dbCursor.sort(sort);
		}
		if(limit!=-1){
			dbCursor.limit(limit);
		}
		List<Record> records = new ArrayList<Record>();
		while (dbCursor.hasNext()) {
			records.add(toRecord(dbCursor.next()));
		}
		return records;
	}
    
	public static int queryCount(String collection, Map<String, Object> filter,
			Map<String, Object> like, Map<String, Object> regex) {
		DBCollection logs = MongoKit.getCollection(collection);
		BasicDBObject conditons = new BasicDBObject();
		buildFilter(filter, conditons);
		buildLike(like, conditons);
		buildRegex(regex, conditons);
		return logs.find(conditons).count();
	}

    private static void sort(Map<String, Object> sort, DBCursor dbCursor) {
        if (sort != null) {
            DBObject dbo = new BasicDBObject();
            Set<Entry<String, Object>> entrySet = sort.entrySet();
            for (Entry<String, Object> entry : entrySet) {
                String key = entry.getKey();
                Object val = entry.getValue();
                dbo.put(key, "asc".equalsIgnoreCase(val + "") ? 1 : -1);
            }
            dbCursor = dbCursor.sort(dbo);
        }
    }

    private static void buildLike(Map<String, Object> like, BasicDBObject conditons) {
        if (like != null) {
            Set<Entry<String, Object>> entrySet = like.entrySet();
            for (Entry<String, Object> entry : entrySet) {
                String key = entry.getKey();
                Object val = entry.getValue();
                conditons.put(key, MongoKit.getLikeStr(val));
            }
        }
    }
    
    /**
     * 生成正则表达式的查询条件
     * @param regex 作为查询条件的正则表达式
     * @param conditons
     */
    private static void buildRegex(Map<String, Object> regex, BasicDBObject conditons) {
        if (regex != null) {
            Set<Entry<String, Object>> entrySet = regex.entrySet();
            for (Entry<String, Object> entry : entrySet) {
                String key = entry.getKey();
                Object val = entry.getValue();
                conditons.put(key, MongoKit.getRegexStr(val));
            }
        }
    }

    private static void buildFilter(Map<String, Object> filter, BasicDBObject conditons) {
        if (filter != null) {
            Set<Entry<String, Object>> entrySet = filter.entrySet();
            for (Entry<String, Object> entry : entrySet) {
                String key = entry.getKey();
                Object val = entry.getValue();
                conditons.put(key, val);
            }

        }
    }

    @SuppressWarnings("unchecked")
    public static Record toRecord(DBObject dbObject) {
        Record record = new Record();
        record.setColumns(dbObject.toMap());
        return record;
    }
    
    @SuppressWarnings("unchecked")
    public static List<Record> toRecords(List<BasicDBObject> dbObjects) {
    	List<Record> records=new ArrayList<Record>();
    	for(DBObject dbObject : dbObjects){
    		Record record = new Record();
            record.setColumns(dbObject.toMap());
            records.add(record);
    	}
        return records;
    }

    public static BasicDBObject getLikeStr(Object findStr) {
        Pattern pattern = Pattern.compile("^.*" + findStr + ".*$", Pattern.CASE_INSENSITIVE);
        return new BasicDBObject("$regex", pattern);
    }
    
    public static BasicDBObject getRegexStr(Object regexStr) {
        Pattern pattern = Pattern.compile(regexStr.toString(), Pattern.CASE_INSENSITIVE);
        return new BasicDBObject("$regex", pattern);
    }

    public static DB getDB() {
        return defaultDb;
    }

    public static DB getDB(String dbName) {
        return client.getDB(dbName);
    }

    public static DBCollection getCollection(String name) {
        return defaultDb.getCollection(name);
    }

    public static DBCollection getDBCollection(String dbName, String collectionName) {
        return getDB(dbName).getCollection(collectionName);
    }

    public static MongoClient getClient() {
        return client;
    }

    public static void setMongoClient(MongoClient client) {
        MongoKit.client = client;
    }

    public static BasicDBObject toDBObject(Map<String, Object> map) {
        BasicDBObject dbObject = new BasicDBObject();
        Set<Entry<String, Object>> entrySet = map.entrySet();
        for (Entry<String, Object> entry : entrySet) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if(val instanceof Map){
            	dbObject.append(key, toDBObject((Map)val));
            }else{
            	if("_id".equals(key)){
            		dbObject.append(key, new ObjectId(val.toString()));
            	}else if(val instanceof BigDecimal){
            		dbObject.append(key, Double.valueOf(val.toString()));
            	}else{
            		dbObject.append(key, val);
            	}
            }
        }
        return dbObject;
    }

    public static BasicDBObject toDbObject(Record record) {
        BasicDBObject object = new BasicDBObject();
        for (Entry<String, Object> e : record.getColumns().entrySet()) {
        	if(e.getValue() instanceof Record){
        		object.append(e.getKey(), toDbObject((Record)e.getValue()));
        	}else{
        		if(e.getValue() instanceof BigDecimal){
        			object.append(e.getKey(), Double.valueOf(e.getValue().toString()));
        		}else{
        			object.append(e.getKey(), e.getValue());
        		}
        		
        	}
        }
        return object;
    }
}
