package org.vertx.mods.mongo.test.integration.org.vertx.mods;

import com.mongodb.ReadPreference;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.vertx.mods.MongoPersistor;

import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class MongoPersistorTest {

  @Test
  public void testMakeConfigurationForClientCorrectlyModifiesConfigurationForPrimary(final TestContext context) {
    final Vertx vertx = Vertx.vertx();
    final JsonObject config = new JsonObject()
      .put("username", "toto")
      .put("password", "pwd")
      .put("authSource", "{{ dbName }}")
      .put("authMechanism", "SCRAM-SHA-1")
      .put("connection_string", "mongodb://toto:tata@mongo1:27017,mongo2:27017/dbname?authSource=authDb&ssl=true&maxPoolSize=110&readPreference=nearest&w=1")
      .put("db_name", "dbname")
      .put("use_mongo_types", true)
      .put("use_ssl", true)
      .put("read_preference", "nearest")
      .put("pool_size", 110);
    final JsonObject newConfig = MongoPersistor.makeConfigurationForClient(config, 10, ReadPreference.primary(), vertx);
    assertEquals("Pool size should have been overridden", 10, (int)newConfig.getInteger("pool_size"));
    assertEquals("Read preference should have been overridden", "primary", newConfig.getString("read_preference"));
    assertEquals("Connection string should have been modifed",
      "mongodb://toto:tata@mongo1:27017,mongo2:27017/dbname?authSource=authDb&ssl=true&maxPoolSize=110&readPreference=primary&w=1",
      newConfig.getString("connection_string"));
  }


  @Test
  public void testMakeConfigurationForClientCorrectlyModifiesConfigurationForDefault(final TestContext context) {
    final Vertx vertx = Vertx.vertx();
    final JsonObject config = new JsonObject()
      .put("username", "toto")
      .put("password", "pwd")
      .put("authSource", "{{ dbName }}")
      .put("authMechanism", "SCRAM-SHA-1")
      .put("connection_string", "mongodb://toto:tata@mongo1:27017,mongo2:27017/dbname?authSource=authDb&ssl=true&maxPoolSize=110&readPreference=nearest&w=1")
      .put("db_name", "dbname")
      .put("use_mongo_types", true)
      .put("use_ssl", true)
      .put("read_preference", "nearest")
      .put("pool_size", 110);
    final JsonObject newConfig = MongoPersistor.makeConfigurationForClient(config, 100, null, vertx);
    assertEquals("Pool size should have been overridden", 100, (int)newConfig.getInteger("pool_size"));
    assertEquals("Read preference should have been overridden", "nearest", newConfig.getString("read_preference"));
    assertEquals("Connection string should have been modifed",
      config.getString("connection_string"),
      newConfig.getString("connection_string"));

  }
}
