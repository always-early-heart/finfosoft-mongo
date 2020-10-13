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
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class MongodbPlugin implements IPlugin {
    
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAUL_PORT = 27017;

    protected final Logger logger = Logger.getLogger(getClass());

    private MongoClient client;
    private String host;
    private int port;
    private String database;
    private String username;
    private String password;

    public MongodbPlugin(String database) {
        this.host = DEFAULT_HOST;
        this.port = DEFAUL_PORT;
        this.database = database;
    }

    public MongodbPlugin(String host, int port, String database) {
        this.host = host;
        this.port = port;
        this.database = database;
    }
    
    public MongodbPlugin(String host, int port, String database,String username,String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public boolean start() {
        try {
        	Builder builder = new MongoClientOptions.Builder();
        	builder.connectionsPerHost(300); // 连接池设置为300个连接,默认为100
        	builder.connectTimeout(3000);    // 连接超时，推荐>3000毫秒
        	builder.maxWaitTime(5000);       //
        	builder.socketTimeout(0);        // 套接字超时时间，0无限制
            builder.threadsAllowedToBlockForConnectionMultiplier(5000);// 线程队列数，如果连接线程排满了队列就会抛出“Out of semaphores to get db”错误。
            builder.writeConcern(WriteConcern.SAFE);//
            MongoClientOptions options=builder.build();

            if(username==null){
            	client = new MongoClient(host, port);
            }else{
            	//MongoCredential credential = MongoCredential.createMongoCRCredential(username, database, password.toCharArray());
            	MongoCredential credential = MongoCredential.createCredential(username, database, password.toCharArray());
            	client = new MongoClient(Arrays.asList(new ServerAddress(host, port)),Arrays.asList(credential), options);
            }
            
        } catch (Exception e) {
            throw new RuntimeException("can't connect mongodb, please check the host and port:" + host + "," + port, e);
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
