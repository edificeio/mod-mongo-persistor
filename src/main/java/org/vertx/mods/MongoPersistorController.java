package org.vertx.mods;

import fr.wseduc.webutils.request.RequestUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;

public class MongoPersistorController extends AbstractVerticle {

    public static final int PORT = 8000;
    public static final String HOST = "localhost";
    public static final String MONGODB_PERSISTOR_ADDRESS = "wse.mongodb.persistor";

    @Override
    public void start(Promise<Void> startPromise) {

        HttpServer httpServer = vertx.createHttpServer();
        EventBus eventBus = vertx.eventBus();

        httpServer.requestHandler(request -> {
            if (request.method() == HttpMethod.POST && request.uri().equals("/sendToEventBus")) {
                RequestUtils.bodyToJson(request, jsonBody -> eventBus.request(MONGODB_PERSISTOR_ADDRESS, jsonBody)
                        .onSuccess(result -> {
                            JsonObject jsonResult = (JsonObject) result.body();
                            if (jsonResult.getString("status").equals("ok")) {
                                request.response()
                                        .putHeader("content-type", "application/json")
                                        .end(jsonResult.toString());
                            } else {
                                request.response().setStatusCode(400).end(jsonResult.getString("message"));
                            }
                        })
                        .onFailure(error -> request.response().setStatusCode(500).end(error.getMessage())));
            } else {
                request.response().setStatusCode(404).end("Not found");
            }
        });

        httpServer.listen(PORT, HOST, result -> {
            if (result.succeeded()) {
                startPromise.complete();
                System.out.println("HTTP server started on host " + HOST + " on port " + PORT);
            } else {
                startPromise.fail(result.cause());
            }
        });
    }
}
