package org.apache.flink.fs.ddb.rule;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.ServerSocket;

public class LocalDbRule extends ExternalResource {

    public static String port;

    static {
        try {
            port = Integer.toString(new ServerSocket(0).getLocalPort());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected DynamoDBProxyServer server;

    public LocalDbRule() {
        System.setProperty("sqlite4java.library.path", "native-libs");
    }

    @Override
    protected void before() throws Exception {
        this.server = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-sharedDb", "-port", port});
        server.start();
    }

    @Override
    protected void after() {
        try {
            server.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}