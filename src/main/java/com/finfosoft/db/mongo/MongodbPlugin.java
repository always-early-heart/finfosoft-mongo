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

import java.util.Arrays;

import com.jfinal.log.Logger;
import com.jfinal.plugin.IPlugin;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
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
