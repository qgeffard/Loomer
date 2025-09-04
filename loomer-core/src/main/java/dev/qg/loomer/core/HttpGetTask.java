package dev.qg.loomer.core;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

/** Simple HTTP GET task. */
public class HttpGetTask implements Task {
  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  public TaskResult run(TaskContext ctx) throws Exception {
    Map<?,?> in = ctx.readInput(Map.class);
    String url = (String) in.get("url");
    HttpRequest req = HttpRequest.newBuilder(URI.create(url)).GET().build();
    HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
    int status = resp.statusCode();
    if (status >= 200 && status < 300) {
      ctx.writeOutput(Map.of("status", status));
      return TaskResult.success();
    }
    if (status >= 500) {
      return TaskResult.retry("5xx");
    }
    return TaskResult.fail("status " + status);
  }
}
