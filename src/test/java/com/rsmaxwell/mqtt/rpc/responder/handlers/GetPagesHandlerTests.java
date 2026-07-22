package com.rsmaxwell.mqtt.rpc.responder.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Map;

import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.rsmaxwell.mqtt.rpc.common.Response;
import com.rsmaxwell.mqtt.rpc.common.Status;

class GetPagesHandlerTests {

    private GetPagesHandler handler;

    @BeforeEach
    void setUp() {
        handler = new GetPagesHandler();
    }

    @Test
    void returnsSuccessfulResponse() throws Exception {
        Response response = handler.handleRequest(
                null,
                Map.of(),
                List.of());

        assertNotNull(response);
        assertEquals(Status.OK, response.status());
        assertFalse(response.quit());
    }

    @Test
    void returnsExpectedPagesPayload() throws Exception {
        Response response = handler.handleRequest(
                null,
                Map.of(),
                List.of());

        assertEquals(
                "[ 'one', 'two', 'three' ]",
                response.payload());
    }

    @Test
    void ignoresContextArgumentsAndUserProperties() throws Exception {
        Object context = new Object();

        Map<String, Object> args = Map.of(
                "unused", "value");

        List<UserProperty> userProperties = List.of(
                new UserProperty("unused", "value"));

        Response response = handler.handleRequest(
                context,
                args,
                userProperties);

        assertEquals(
                new Response(
                        Status.OK,
                        "[ 'one', 'two', 'three' ]",
                        false),
                response);
    }
}

