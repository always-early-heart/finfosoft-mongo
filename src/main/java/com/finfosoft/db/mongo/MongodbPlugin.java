package com.finfosoft.db.mongo;

import com.jfinal.log.Logger;
import com.jfinal.plugin.IPlugin;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongodbPlugin implements IPlugin {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAUL_PORT = 27017;

    protected final Logger logger = Logger.getLogger(getClass());

    private MongoClient client;
    private String url;
    private String database;
    private String username;
    private String password;

    public MongodbPlugin(String url, String database) {
        this.url = url;
        this.database = database;
    }

    public MongodbPlugin(String url, String database, String username, String password) {
        this.url = url;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public boolean start() {
        try {
            if (username == null) {
                client = MongoClients.create(url);
            } else {
                MongoCredential credential = MongoCredential.createCredential(username, database,
                        password.toCharArray());
                ConnectionString connectionString = new ConnectionString(url);
                MongoClientSettings settings = MongoClientSettings.builder().credential(credential)
                        .applyConnectionString(connectionString).build();
                client = MongoClients.create(settings);
            }

        } catch (Exception e) {
            throw new RuntimeException("can't connect mongodb, please check the host and url:" + url, e);
        }

        MongoKit.init(client, database);
        return true;
    }

    public boolean stop() {
        if (client != null) {
            client.close();
        }
        return true;
    }

}
