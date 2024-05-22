/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.vertx.mods;

import com.mongodb.DBRef;
import com.mongodb.ReadPreference;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.mongo.*;
import org.vertx.java.busmods.BusModBase;

import java.util.*;
import java.util.stream.Collectors;

/**
 * MongoDB Persistor Bus Module<p>
 * Please see the README.md for a full description<p>
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author Thomas Risberg
 * @author Richard Warburton
 */
public class MongoPersistor extends BusModBase implements Handler<Message<JsonObject>> {

  protected String address;
  protected String host;
  protected int port;
  protected String dbName;
  protected String dbAuth;
  protected String username;
  protected String password;
  protected ReadPreference readPreference;

  protected MongoClient mongo;
  //protected MongoDatabase db;
  private boolean useMongoTypes;
  private WriteOption defaultWriteConcern = WriteOption.ACKNOWLEDGED;

  @Override
  public void start() {
    super.start();

    address = getOptionalStringConfig("address", "vertx.mongopersistor");

    // TODO Vertx4 set this as module config
    /*host = getOptionalStringConfig("host", "localhost");
    port = getOptionalIntConfig("port", 27017);
    dbName = getOptionalStringConfig("db_name", "default_db");
    dbAuth = getOptionalStringConfig("db_auth", "default_db");*/
    username = getOptionalStringConfig("username", null);
    password = getOptionalStringConfig("password", null);
    readPreference = ReadPreference.valueOf(getOptionalStringConfig("read_preference", "primary"));
    /*int poolSize = getOptionalIntConfig("pool_size", 10);
    socketTimeout = getOptionalIntConfig("socket_timeout", 60000);
    useSSL = getOptionalBooleanConfig("use_ssl", false);*/
    useMongoTypes = getOptionalBooleanConfig("use_mongo_types", false);
    defaultWriteConcern = WriteOption.valueOf(getOptionalStringConfig("write_concern", WriteOption.ACKNOWLEDGED.name()));

    JsonArray seedsProperty = config.getJsonArray("seeds");

      this.mongo = MongoClient.createShared(vertx, config);
      /*MongoClientSettings.Builder builder = MongoClientSettings.builder();
      builder.readPreference(readPreference)
        .applyConnectionString(new ConnectionString(config.getString("db_url")));

      final List<MongoCredential> credentials = new ArrayList<>();
      if (username != null && password != null && dbAuth != null) {
        credentials.add(MongoCredential.createScramSha1Credential(username, dbAuth, password.toCharArray()));
      }

      if (seedsProperty == null) {
        ServerAddress address = new ServerAddress(host, port);
        final MongoDriverInformation driverInformation = MongoDriverInformation.builder()
          .driverName("com.mongodb.Mongo")
          .build();
        mongo = new MongoClientImpl(builder.build(), driverInformation);
      } else {
        List<ServerAddress> seeds = makeSeeds(seedsProperty);
        mongo = new MongoClient(seeds, credentials, builder.build());
      }

      db = mongo.getDB(dbName);*/
    eb.consumer(address, this);
  }

  @Override
  public void stop() {
    if (mongo != null) {
      mongo.close();
    }
  }

  @Override
  public void handle(Message<JsonObject> message) {
    String action = message.body().getString("action");

    if (action == null) {
      sendError(message, "action must be specified");
      return;
    }
    try {
      // Note actions should not be in camel case, but should use underscores
      // I have kept the version with camel case so as not to break compatibility

      switch (action) {
        case "save":
          doSave(message);
          break;
        case "update":
          doUpdate(message);
          break;
        case "bulk":
          doBulk(message);
          break;
        case "find":
          doFind(message);
          break;
        case "findone":
          doFindOne(message);
          break;
        // no need for a backwards compatible "findAndModify" since this feature was added after
        case "find_and_modify":
          doFindAndModify(message);
          break;
        case "delete":
          doDelete(message);
          break;
        case "count":
          doCount(message);
          break;
        case "getCollections":
        case "get_collections":
          getCollections(message);
          break;
        case "dropCollection":
        case "drop_collection":
          dropCollection(message);
          break;
        case "collectionStats":
        case "collection_stats":
          getCollectionStats(message);
          break;
        case "aggregate":
          doAggregation(message);
          break;
        case "command":
          runCommand(message);
          break;
        case "distinct":
          doDistinct(message);
          break;
        case "insert":
          doInsert(message);
          break;
        default:
          sendError(message, "Invalid action: " + action);
      }
    } catch (Exception e) {
      sendError(message, e.getMessage(), e);
    }
  }

  private Future<Void> doSave(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.succeededFuture();
    }
    JsonObject doc = getMandatoryObject("document", message);
    if (doc == null) {
      return Future.succeededFuture();
    }
    String genID = generateId(doc);
    // TODO vertx 4 change writeConcern by write_concern in config
    return mongo.saveWithOptions(collection, doc, getWriteConcern())
    .onSuccess(result -> {
      if (genID != null) {
        JsonObject reply = new JsonObject().put("_id", genID);
        sendOK(message, reply);
      } else {
        sendOK(message);
      }
    })
    .onFailure(th -> sendError(message, th.getMessage(), th))
    .mapEmpty();
  }

  private @Nullable WriteOption getWriteConcern() {
    return getWriteConcern(defaultWriteConcern);
  }
  private @Nullable WriteOption getWriteConcern(final WriteOption defaultWriteConcern) {
    Optional<String> writeConcern = getStringConfig("writeConcern");
    if(!writeConcern.isPresent()) {
      writeConcern = getStringConfig("write_concern");
    }
    return writeConcern.map(WriteOption::valueOf).orElse(defaultWriteConcern);
  }

  private Future<Void> doInsert(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.succeededFuture();
    }
    boolean multipleDocuments = message.body().getBoolean("multiple", false);

    List<BulkOperation> operations = new ArrayList<>();
    final StringBuilder genID = new StringBuilder();
    if (multipleDocuments) {
      JsonArray documents = message.body().getJsonArray("documents");
      if (documents == null) {
        return Future.succeededFuture();
      }
      for (Object o : documents) {
        JsonObject doc = (JsonObject) o;
        generateId(doc);
        operations.add(BulkOperation.createInsert(doc));
      }
    } else {
      JsonObject doc = getMandatoryObject("document", message);
      if (doc == null) {
        return Future.succeededFuture();
      }
      genID.append(generateId(doc));
      operations.add(BulkOperation.createInsert(doc));
    }
    try {
      return mongo.bulkWriteWithOptions(collection, operations, new BulkWriteOptions().setWriteOption(WriteOption.ACKNOWLEDGED))
        .onFailure(th -> sendError(message, th.getMessage(), th))
        .onSuccess(res -> {
          JsonObject reply = new JsonObject();
          reply.put("number", res.getInserts().size());
          if (genID.length() > 0) {
            reply.put("_id", genID.toString());
          }
          sendOK(message, reply);
        })
        .mapEmpty();
    } catch (Exception e){
      sendError(message, e.getMessage());
      return Future.failedFuture(e);
    }
  }

  private String generateId(JsonObject doc) {
    if (doc.getValue("_id") == null) {
      String genID = UUID.randomUUID().toString();
      doc.put("_id", genID);
      return genID;
    }
    return null;
  }

  private Future<Void> doUpdate(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.succeededFuture();
    }
    JsonObject criteriaJson = getMandatoryObject("criteria", message);
    if (criteriaJson == null) {
      return Future.succeededFuture();
    }
    JsonObject objNewJson = getMandatoryObject("objNew", message);
    if (objNewJson == null) {
      return Future.succeededFuture();
    }
    final boolean upsert = message.body().getBoolean("upsert", false);
    final boolean multi = message.body().getBoolean("multi", false);
    final UpdateOptions options = new UpdateOptions()
      .setUpsert(upsert)
      .setWriteOption(getWriteConcern())
      .setMulti(multi);
    return mongo.updateCollectionWithOptions(collection, criteriaJson, objNewJson, options)
      .onSuccess(res -> {
        JsonObject reply = new JsonObject();
        reply.put("number", res.getDocModified());
        sendOK(message, reply);
      })
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .mapEmpty();
  }

  private Future<Void> doBulk(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.succeededFuture();
    }
    JsonArray commands = message.body().getJsonArray("commands");
    if (commands == null || commands.isEmpty()) {
      sendError(message, "Missing commands");
      return Future.failedFuture("mongodb.missing.commands");
    }
    final List<BulkOperation> bulk = new ArrayList<>();
    for (Object o: commands) {
      if (!(o instanceof JsonObject)) continue;
      JsonObject command = (JsonObject) o;
      JsonObject d = command.getJsonObject("document");
      JsonObject c = command.getJsonObject("criteria");
      switch (command.getString("operation", "")) {
        case "insert" :
          if (d != null) {
            bulk.add(BulkOperation.createInsert(d));
          }
          break;
        case "update" :
          if (d != null && c != null) {
            bulk.add(BulkOperation.createUpdate(c, d));
          }
          break;
        case "updateOne":
          if (d != null) {
            bulk.add(BulkOperation.createUpdate(c, d, false, false));
          }
          break;
        case "upsert" :
          if (d != null) {
            bulk.add(BulkOperation.createUpdate(c, d, true, true));
          }
          break;
        case "upsertOne":
          if (d != null && c != null) {
            bulk.add(BulkOperation.createUpdate(c, d, true, false));
          }
          break;
        case "remove":
          if (c != null) {
            bulk.add(BulkOperation.createDelete(c));
          }
          break;
        case "removeOne":
          if (c != null) {
            bulk.add(BulkOperation.createDelete(c).setMulti(false));
          }
          break;
      }
    }
    return mongo.bulkWriteWithOptions(collection, bulk, new BulkWriteOptions().setWriteOption(getWriteConcern()))
      .onSuccess(res -> sendOK(message, new JsonObject()
        .put("inserted", res.getInsertedCount())
        .put("matched", res.getMatchedCount())
        .put("modified", res.getModifiedCount())
        .put("removed", res.getDeletedCount())
      ))
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .mapEmpty();
  }

  private Future<Void> doFind(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.succeededFuture();
    }
    Integer limit = message.body().getInteger("limit");
    if (limit == null) {
      limit = -1;
    }
    Integer skip = message.body().getInteger("skip");
    if (skip == null) {
      skip = -1;
    }
    Integer batchSize = message.body().getInteger("batch_size");
    if (batchSize == null) {
      batchSize = 100;
    }
    Integer timeout = message.body().getInteger("timeout");
    if (timeout == null || timeout < 0) {
      timeout = 10000; // 10 seconds
    }
    JsonObject matcher = message.body().getJsonObject("matcher");
    JsonObject keys = message.body().getJsonObject("keys");

    Object hint = message.body().getValue("hint");
    Object sort = message.body().getValue("sort");
    // TODO Vertx4 check how read preferences can be handled with this client
    // add read preference
    /*final String overrideReadPreference = message.body().getString("read_preference");
    if(overrideReadPreference != null){
      coll.setReadPreference(ReadPreference.valueOf(overrideReadPreference));
    }*/
    // call find
    final FindOptions options = new FindOptions();
    if (matcher != null && keys != null) {
      options.setFields(keys);
    }
    if (skip != -1) {
      options.setSkip(skip);
    }
    if (limit != -1) {
      options.setLimit(limit);
    }
    if (sort != null) {
      options.setSort((JsonObject) sort);
    }
    if (hint != null) {
      if (hint instanceof JsonObject) {
        options.setHint((JsonObject) hint);
      } else if (hint instanceof String) {
        options.setHintString((String) hint);
      } else {
        return Future.failedFuture("Cannot handle type " + hint.getClass().getSimpleName());
      }
    }
    return mongo.findWithOptions(collection, matcher == null ? new JsonObject() : matcher, options)
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(results -> message.reply(createBatchMessage("ok", results)))
      .mapEmpty();
  }

  private JsonObject createBatchMessage(String status, final List<?> results) {
    JsonObject reply = new JsonObject();
    reply.put("results", results);
    reply.put("status", status);
    reply.put("number", results.size());
    return reply;
  }

  private Future<Void> doFindOne(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    JsonObject matcher = message.body().getJsonObject("matcher");
    JsonObject keys = message.body().getJsonObject("keys");
    // add read preference
    // TODO Vertx4 check how read preferences can be handled with this client
    /*final String overrideReadPreference = message.body().getString("read_preference");
    if(overrideReadPreference != null){
      coll.setReadPreference(ReadPreference.valueOf(overrideReadPreference));
    }*/
    return mongo.findOne(collection, matcher == null ? new JsonObject() : matcher, keys)
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .compose(res -> {
        JsonObject reply = new JsonObject();
        JsonArray fetch = message.body().getJsonArray("fetch");
        final Promise<JsonObject> promise = Promise.promise();
        if (res == null) {
          promise.complete(reply);
        } else {
          reply.put("result", res);
          if (fetch == null) {
            promise.complete(reply);
          } else {
            List<Future<?>> fetches = fetch.stream().map(attr -> {
              if (attr instanceof String) {
                String f = (String) attr;
                Object tmp = res.getValue(f);
                final Future<Void> onDbRefFetched;
                if (tmp == null || !(tmp instanceof DBRef)) {
                  onDbRefFetched = Future.succeededFuture();
                } else {
                  onDbRefFetched = fetchRef((DBRef) tmp)
                    .onSuccess(fetched -> res.put(f, fetched))
                    .mapEmpty();
                }
                return onDbRefFetched;
              } else {
                return Future.succeededFuture(null);
              }
            }).collect(Collectors.toList());
            Future.all(fetches).onSuccess(e -> promise.complete()).onFailure(promise::fail);
          }
        }
        return promise.future();
      })
      .onSuccess(reply -> sendOK(message, reply))
      .mapEmpty();
  }

  private Future<JsonObject> fetchRef(final DBRef ref){
    final JsonObject query = new JsonObject().put("_id", ref.getId());
    return mongo.findOne(ref.getCollectionName(), query, null);
  }

  private Future<Void> doFindAndModify(Message<JsonObject> message) {
    String collectionName = getMandatoryString("collection", message);
    if (collectionName == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    final JsonObject msgBody = message.body();
    final JsonObject update = msgBody.getJsonObject("update");
    final JsonObject query = msgBody.getJsonObject("matcher");
    final JsonObject sort = msgBody.getJsonObject("sort");
    final JsonObject fields = msgBody.getJsonObject("fields");
    // TODO vertx 4 check how to use that
    //final JsonObject result = collection.findAndModify(query, fields, sort, remove,
    //  update, returnNew, upsert);
    boolean remove = msgBody.getBoolean("remove", false);
    boolean returnNew = msgBody.getBoolean("new", false);
    boolean upsert = msgBody.getBoolean("upsert", false);

    final FindOptions findOptions = new FindOptions().setSort(sort).setFields(fields);
    final UpdateOptions updateOptions = new UpdateOptions()
      .setReturningNewDocument(returnNew)
      .setUpsert(upsert);
    return mongo.findOneAndUpdateWithOptions(collectionName, query, update, findOptions, updateOptions)
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(res -> {
        final JsonObject reply = new JsonObject();
        if (res != null) {
          reply.put("result", res);
        }
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  private Future<Void> doCount(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    final JsonObject matcher = message.body().getJsonObject("matcher");

    // TODO Vertx4 check how read preferences can be handled with this client
    // add read preference
    /*final String overrideReadPreference = message.body().getString("read_preference");
    if(overrideReadPreference != null){
      coll.setReadPreference(ReadPreference.valueOf(overrideReadPreference));
    }*/
    // call find
    return mongo.count(collection, matcher == null ? new JsonObject() : matcher)
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(count -> {
        JsonObject reply = new JsonObject();
        reply.put("count", count);
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Future<Void> doDistinct(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    final String key = getMandatoryString("key", message);
    if (key == null) {
      return Future.failedFuture("collection.key.mandatory");
    }
    final JsonObject matcher = message.body().getJsonObject("matcher");
    final Future<JsonArray> future;
    if(matcher == null) {
      future = mongo.distinct(collection, key, String.class.getName());
    } else {
      future = mongo.distinctWithQuery(collection, key, String.class.getName(), matcher);
    }
    return future.
      onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(values -> {
        JsonObject reply = new JsonObject().put("values", values);
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  private Future<Void> doDelete(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    JsonObject matcher = getMandatoryObject("matcher", message);
    if (matcher == null) {
      return Future.failedFuture("matcher.mandatory");
    }
    return mongo.removeDocumentWithOptions(collection, matcher, getWriteConcern())
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(res -> {
        JsonObject reply = new JsonObject().put("number", res.getRemovedCount());
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  private Future<Void> getCollections(Message<JsonObject> message) {
    return mongo.getCollections()
      .onFailure(th -> sendError(message, th.getMessage(), th))
      .onSuccess(colls -> {
        final JsonObject reply = new JsonObject().put("collections", new JsonArray(colls));
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  private Future<Void> dropCollection(Message<JsonObject> message) {
    String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    return mongo.dropCollection(collection)
      .onFailure(th -> sendError(message, "exception thrown when attempting to drop collection: " + collection + "\n" + th.getMessage(), th))
      .onSuccess(e -> sendOK(message, new JsonObject()));
  }

  // TODO Vertx4 not sure it works
  private Future<Void> getCollectionStats(Message<JsonObject> message) {
    final String collection = getMandatoryString("collection", message);
    if (collection == null) {
      return Future.failedFuture("collection.name.mandatory");
    }
    return mongo.runCommand("stats", new JsonObject())
      .onFailure(th -> sendError(message, th))
      .onSuccess(stats -> {
        final JsonObject reply = new JsonObject().put("stats", stats);
        sendOK(message, reply);
      })
      .mapEmpty();
  }

  private Future<Void> doAggregation(Message<JsonObject> message) {
    if (isCollectionMissing(message)) {
      return Future.failedFuture("collection.name.mandatory");
    }
    if (isPipelinesMissing(message.body().getJsonArray("pipelines"))) {
      sendError(message, "no pipeline operations found");
      return Future.failedFuture("pipelines.mandatory");
    }
    final String collection = message.body().getString("collection");
    final JsonArray pipelines = message.body().getJsonArray("pipelines");
    final ReadStream<JsonObject> stream = mongo.aggregate(collection, pipelines);
    final Promise<Void> promise = Promise.promise();
    final JsonArray results = new JsonArray();
    stream.endHandler(e -> {
      JsonObject reply = new JsonObject();
      reply.put("results", results);
      sendOK(message, reply);
      promise.complete();
    })
    .handler(results::add)
    .exceptionHandler(th -> sendError(message, th)); // TODO Vertx4 check if this is a terminal operation
    return promise.future();
  }


  private boolean isCollectionMissing(Message<JsonObject> message) {
    return getMandatoryString("collection", message) == null;
  }

  private boolean isPipelinesMissing(JsonArray pipelines) {
    return pipelines == null || pipelines.size() == 0;
  }

  private Future<Void> runCommand(Message<JsonObject> message) {
    JsonObject reply = new JsonObject();
    final String commandRaw = getMandatoryString("command", message);
    if (commandRaw == null) {
      return Future.failedFuture("command.mandatory");
    }
    try {
      final JsonObject command = new JsonObject(commandRaw);
      return getCommandName(command).map(commandName -> mongo.runCommand(commandName, command)
          .onFailure(th -> sendError(message, th))
          .onSuccess(result -> {
            result.remove("operationTime");
            result.remove("$clusterTime");
            result.remove("opTime");
            result.remove("electionId");
            reply.put("result", result);
            sendOK(message, reply);
          })).orElseGet(() -> Future.failedFuture("command.name.mandatory"))
        .mapEmpty();
    } catch (Exception th) {
      sendError(message, th);
      return Future.failedFuture(th);
    }
  }

  private Optional<String> getCommandName(final JsonObject command) {
    return command.fieldNames().stream().findFirst();
  }

}

