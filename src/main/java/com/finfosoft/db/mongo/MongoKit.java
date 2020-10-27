package com.finfosoft.db.mongo;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.jfinal.log.Logger;
import com.jfinal.plugin.activerecord.Page;
import com.jfinal.plugin.activerecord.Record;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoKit {

    protected static Logger logger = Logger.getLogger(MongoKit.class);

    private static MongoClient client;
    private static MongoDatabase defaultDb;

    public static void init(final MongoClient client, final String database) {
        MongoKit.client = client;
        MongoKit.defaultDb = client.getDatabase(database);

    }

    public static void updateFirst(final String collectionName, final Map<String, Object> q,
            final Map<String, Object> o) {
        MongoKit.getCollection(collectionName).findOneAndUpdate(toDBObject(q), toDBObject(o));
    }

    public static void updateFirst(final String collectionName, final Map<String, Object> q, final Record o) {
        MongoKit.getCollection(collectionName).findOneAndUpdate(toDBObject(q), toDbObject(o));
    }

    public static int getUniqueId(final String idName) {
        final BasicDBObject query = new BasicDBObject().append("name", idName);
        final BasicDBObject update = new BasicDBObject().append("$inc", new BasicDBObject().append("id", 1));
        final BasicDBObject ids = toDBObject(MongoKit.getCollection("ids").findOneAndUpdate(query, update));
        final int id = ids.getInt("id");
        return id;
    }

    public static long removeAll(final String collectionName) {
        return MongoKit.getCollection(collectionName).deleteMany(new BasicDBObject()).getDeletedCount();
    }

    public static long remove(final String collectionName, final Map<String, Object> filter) {
        return MongoKit.getCollection(collectionName).deleteMany(toDBObject(filter)).getDeletedCount();
    }

    public static boolean save(final String collectionName, final List<Record> records) {
        final List<Document> objs = new ArrayList<Document>();
        for (final Record record : records) {
            objs.add(toDbObject(record));
        }
        return MongoKit.getCollection(collectionName).insertMany(objs).wasAcknowledged();
    }

    public static boolean save(final String collectionName, final Record record) {
        return save(collectionName, toDbObject(record));
    }

    public static boolean save(final String collectionName, final Document record) {
        if(record.get("_id")!=null){
            return MongoKit.getCollection(collectionName).replaceOne(new BasicDBObject("_id", record.get("_id")), record).wasAcknowledged();
        }else{
            return MongoKit.getCollection(collectionName).insertOne(record).wasAcknowledged();
        }
    }

    public static boolean save(final String collectionName, final Map<String, Object> record) {
        return save(collectionName, toDBObject(record));
    }

    public static boolean save(final String collectionName, final BasicDBObject record) {
        return save(collectionName,toRecord(record));
    }

    public static Record findFirst(final String collectionName) {
        return toRecord(MongoKit.getCollection(collectionName).find().first());
    }

    public static Record findById(final String collectionName, final String id) {
        final BasicDBObject conditons = new BasicDBObject();
        conditons.put("_id", new ObjectId(id));
        return toRecord(MongoKit.getCollection(collectionName).find(conditons).first());
    }

    public static Record findById(final String collectionName, final int id) {
        final BasicDBObject conditons = new BasicDBObject();
        conditons.put("_id", id);
        return toRecord(MongoKit.getCollection(collectionName).find(conditons).first());
    }

    public static Record findById(final String collectionName, final ObjectId _id) {
        final BasicDBObject conditons = new BasicDBObject();
        conditons.put("_id", _id);
        return toRecord(MongoKit.getCollection(collectionName).find(conditons).first());
    }

    public static Page<Record> paginate(final String collection, final int pageNumber, final int pageSize) {
        return paginate(collection, pageNumber, pageSize, null, null, null, null);
    }

    public static Page<Record> paginate(final String collection, final int pageNumber, final int pageSize,
            final Map<String, Object> filter) {
        return paginate(collection, pageNumber, pageSize, filter, null, null, null);
    }

    public static Page<Record> paginate(final String collection, final int pageNumber, final int pageSize,
            final Map<String, Object> filter, final Map<String, Object> like) {
        return paginate(collection, pageNumber, pageSize, filter, like, null, null);
    }

    public static Page<Record> paginate(final String collection, final int pageNumber, final int pageSize,
            final Map<String, Object> filter, final Map<String, Object> like, final Map<String, Object> sort) {
        return paginate(collection, pageNumber, pageSize, filter, like, sort, null);
    }

    public static Page<Record> paginate(final String collection, final int pageNumber, final int pageSize,
            final Map<String, Object> filter, final Map<String, Object> like, final Map<String, Object> sort,
            final Map<String, Object> regex) {
        final MongoCollection<Document> logs = MongoKit.getCollection(collection);
        final BasicDBObject conditons = new BasicDBObject();
        buildFilter(filter, conditons);
        buildLike(like, conditons);
        buildRegex(regex, conditons);
        final FindIterable<Document> findIterable = logs.find(conditons);
        final long totalRow=logs.countDocuments(conditons);

        page(pageNumber, pageSize, findIterable);
        sort(sort, findIterable);

        final MongoCursor<Document> dbCursor = findIterable.cursor();
        final List<Record> records = new ArrayList<Record>();
        while (dbCursor.hasNext()) {
            records.add(toRecord(dbCursor.next()));
        }
        if (totalRow <= 0) {
            return new Page<Record>(new ArrayList<Record>(0), pageNumber, pageSize, 0, 0);
        }
        long totalPage = totalRow / pageSize;
        if (totalRow % pageSize != 0) {
            totalPage++;
        }
        final Page<Record> page = new Page<Record>(records, pageNumber, pageSize, new Long(totalPage).intValue(),new Long(totalRow).intValue());
        return page;
    }

    private static void page(final int pageNumber, final int pageSize, FindIterable<Document> findIterable) {
        findIterable = findIterable.skip((pageNumber - 1) * pageSize).limit(pageSize);
    }

    public static List<Record> query(final String collection, final Map<String, Object> filter) {
        return query(collection, filter, null, null, null);
    }

    public static List<Record> query(final String collection, final Map<String, Object> filter,
            final Map<String, Object> like) {
        return query(collection, filter, like, null, null);
    }

    public static List<Record> query(final String collection, final Map<String, Object> filter,
            final Map<String, Object> like, final Map<String, Object> sort) {
        return query(collection, filter, like, sort, null);
    }

    public static List<Record> query(final String collection, final Map<String, Object> filter,
            final Map<String, Object> like, final Map<String, Object> sort, final Map<String, Object> regex) {
        final MongoCollection<Document> logs = MongoKit.getCollection(collection);
        final BasicDBObject conditons = new BasicDBObject();
        buildFilter(filter, conditons);
        buildLike(like, conditons);
        buildRegex(regex, conditons);
        final FindIterable<Document> findIterable = logs.find(conditons);
        sort(sort, findIterable);
        final MongoCursor<Document> dbCursor = findIterable.cursor();
        final List<Record> records = new ArrayList<Record>();
        while (dbCursor.hasNext()) {
            records.add(toRecord(dbCursor.next()));
        }
        return records;
    }

    /**
     * 刘鹏飞 新增 可进行正则排序的方法
     * 
     * @param collection
     * @param conditons
     * @param sort
     * @param regex
     * @param limit
     * @return
     */
    public static List<Record> query(final String collection, final BasicDBObject conditons, final BasicDBObject sort,
            final BasicDBObject regex, final int limit) {
        final MongoCollection<Document> logs = MongoKit.getCollection(collection);
        if (regex != null) {
            final Set<Entry<String, Object>> entrySet = regex.entrySet();
            for (final Entry<String, Object> entry : entrySet) {
                final String key = entry.getKey();
                final Object val = entry.getValue();
                conditons.put(key, MongoKit.getRegexStr(val));
            }
        }
        final FindIterable<Document> findIterable = logs.find(conditons);
        if (sort != null) {
            findIterable.sort(sort);
        }
        if (limit != -1) {
            findIterable.limit(limit);
        }
        final MongoCursor<Document> dbCursor = findIterable.cursor();
        final List<Record> records = new ArrayList<Record>();
        while (dbCursor.hasNext()) {
            records.add(toRecord(dbCursor.next()));
        }
        return records;
    }

    public static List<Record> query(final String collection, final Map<String, Object> filter,
            final Map<String, Object> like, final Map<String, Object> sort, final Map<String, Object> regex,
            final int limit, final BasicDBObject projection) {
        final MongoCollection<Document> logs = MongoKit.getCollection(collection);
        final BasicDBObject conditons = new BasicDBObject();
        buildFilter(filter, conditons);
        buildLike(like, conditons);
        buildRegex(regex, conditons);
        FindIterable<Document> findIterable;

        if (projection != null) {
            findIterable = logs.find(conditons).projection(projection);
        } else {
            findIterable = logs.find(conditons);
        }
        sort(sort, findIterable);
        if (limit != -1) {
            findIterable.limit(limit);
        }
        final MongoCursor<Document> dbCursor = findIterable.cursor();
        final List<Record> records = new ArrayList<Record>();
        while (dbCursor.hasNext()) {
            records.add(toRecord(dbCursor.next()));
        }
        return records;
    }

    public static List<Record> query(final String collection, final BasicDBObject conditons,
            final Map<String, Object> sort) {
        final MongoCollection<Document> logs = MongoKit.getCollection(collection);
        final FindIterable<Document> findIterable = logs.find(conditons);
        sort(sort, findIterable);
        final MongoCursor<Document> dbCursor = findIterable.cursor();
        final List<Record> records = new ArrayList<Record>();
        while (dbCursor.hasNext()) {
            records.add(toRecord(dbCursor.next()));
        }
        return records;
    }

    public static List<Record> query(final String collection, final BasicDBObject conditons, final BasicDBObject sort) {
        final List<Record> records = query(collection, conditons, sort, -1);
        return records;
    }

    public static List<Record> query(final String collection, final BasicDBObject conditons, final BasicDBObject sort,
            final int limit) {
        final MongoCollection<Document> logs = MongoKit.getCollection(collection);
        final FindIterable<Document> findIterable = logs.find(conditons);
        if (sort != null) {
            findIterable.sort(sort);
        }
        if (limit != -1) {
            findIterable.limit(limit);
        }
        final MongoCursor<Document> dbCursor = findIterable.cursor();
        final List<Record> records = new ArrayList<Record>();
        while (dbCursor.hasNext()) {
            records.add(toRecord(dbCursor.next()));
        }
        return records;
    }

    public static long queryCount(final String collection, final Map<String, Object> filter,
            final Map<String, Object> like, final Map<String, Object> regex) {
        final MongoCollection<Document> logs = MongoKit.getCollection(collection);
        final BasicDBObject conditons = new BasicDBObject();
        buildFilter(filter, conditons);
        buildLike(like, conditons);
        buildRegex(regex, conditons);
        return logs.countDocuments(conditons);
    }

    private static void sort(final Map<String, Object> sort, FindIterable<Document> dbCursor) {
        if (sort != null) {
            final BasicDBObject dbo = new BasicDBObject();
            final Set<Entry<String, Object>> entrySet = sort.entrySet();
            for (final Entry<String, Object> entry : entrySet) {
                final String key = entry.getKey();
                final Object val = entry.getValue();
                dbo.put(key, "asc".equalsIgnoreCase(val + "") ? 1 : -1);
            }
            dbCursor = dbCursor.sort(dbo);
        }
    }

    private static void buildLike(final Map<String, Object> like, final BasicDBObject conditons) {
        if (like != null) {
            final Set<Entry<String, Object>> entrySet = like.entrySet();
            for (final Entry<String, Object> entry : entrySet) {
                final String key = entry.getKey();
                final Object val = entry.getValue();
                conditons.put(key, MongoKit.getLikeStr(val));
            }
        }
    }

    /**
     * 生成正则表达式的查询条件
     * 
     * @param regex     作为查询条件的正则表达式
     * @param conditons
     */
    private static void buildRegex(final Map<String, Object> regex, final BasicDBObject conditons) {
        if (regex != null) {
            final Set<Entry<String, Object>> entrySet = regex.entrySet();
            for (final Entry<String, Object> entry : entrySet) {
                final String key = entry.getKey();
                final Object val = entry.getValue();
                conditons.put(key, MongoKit.getRegexStr(val));
            }
        }
    }

    private static void buildFilter(final Map<String, Object> filter, final BasicDBObject conditons) {
        if (filter != null) {
            final Set<Entry<String, Object>> entrySet = filter.entrySet();
            for (final Entry<String, Object> entry : entrySet) {
                final String key = entry.getKey();
                final Object val = entry.getValue();
                conditons.put(key, val);
            }

        }
    }

    @SuppressWarnings("unchecked")
    public static Record toRecord(final DBObject dbObject) {
        final Record record = new Record();
        record.setColumns(dbObject.toMap());
        return record;
    }

    @SuppressWarnings("unchecked")
    public static Record toRecord(final Document dbObject) {
        final Map<String, Object> map = new HashMap<String, Object>();
        map.putAll(dbObject);
        final Record record = new Record();
        record.setColumns(map);
        return record;
    }

    @SuppressWarnings("unchecked")
    public static List<Record> toRecords(final List<BasicDBObject> dbObjects) {
        final List<Record> records = new ArrayList<Record>();
        for (final DBObject dbObject : dbObjects) {
            final Record record = new Record();
            record.setColumns(dbObject.toMap());
            records.add(record);
        }
        return records;
    }

    public static BasicDBObject getLikeStr(final Object findStr) {
        final Pattern pattern = Pattern.compile("^.*" + findStr + ".*$", Pattern.CASE_INSENSITIVE);
        return new BasicDBObject("$regex", pattern);
    }

    public static BasicDBObject getRegexStr(final Object regexStr) {
        final Pattern pattern = Pattern.compile(regexStr.toString(), Pattern.CASE_INSENSITIVE);
        return new BasicDBObject("$regex", pattern);
    }

    public static MongoDatabase getDB() {
        return defaultDb;
    }

    public static MongoDatabase getDB(final String dbName) {
        return client.getDatabase(dbName);
    }

    public static MongoCollection<Document> getCollection(final String name) {
        return defaultDb.getCollection(name);
    }

    public static MongoCollection<Document> getDBCollection(final String dbName, final String collectionName) {
        return getDB(dbName).getCollection(collectionName);
    }

    public static MongoClient getClient() {
        return client;
    }

    public static void setMongoClient(final MongoClient client) {
        MongoKit.client = client;
    }

    public static Document toDbObject(final Record record) {
        final Map<String, Object> object = new HashMap<>();
        for (final Entry<String, Object> e : record.getColumns().entrySet()) {
            if (e.getValue() instanceof Record) {
                object.put(e.getKey(), toDbObject((Record) e.getValue()));
            } else {
                if (e.getValue() instanceof BigDecimal) {
                    object.put(e.getKey(), Double.valueOf(e.getValue().toString()));
                } else {
                    object.put(e.getKey(), e.getValue());
                }

            }
        }
        return new Document(object);
    }

    public static BasicDBObject toDBObject(final Map<String, Object> map) {
        final BasicDBObject dbObject = new BasicDBObject();
        final Set<Entry<String, Object>> entrySet = map.entrySet();
        for (final Entry<String, Object> entry : entrySet) {
            final String key = entry.getKey();
            final Object val = entry.getValue();
            if (val instanceof Map) {
                dbObject.append(key, toDBObject((Map) val));
            } else {
                if ("_id".equals(key)) {
                    dbObject.append(key, new ObjectId(val.toString()));
                } else if (val instanceof BigDecimal) {
                    dbObject.append(key, Double.valueOf(val.toString()));
                } else {
                    dbObject.append(key, val);
                }
            }
        }
        return dbObject;
    }
}
