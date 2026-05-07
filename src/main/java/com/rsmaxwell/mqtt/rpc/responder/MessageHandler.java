package com.rsmaxwell.mqtt.rpc.responder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rsmaxwell.mqtt.rpc.common.Adapter;
import com.rsmaxwell.mqtt.rpc.common.Request;
import com.rsmaxwell.mqtt.rpc.common.Response;
import com.rsmaxwell.mqtt.rpc.common.Status;
import com.rsmaxwell.mqtt.rpc.exceptions.RpcStatusException;
import com.rsmaxwell.mqtt.rpc.responder.buildinfo.BuildInfo;

import io.jsonwebtoken.ExpiredJwtException;

public class MessageHandler extends Adapter implements MqttCallback {

	private static final Logger log = LoggerFactory.getLogger(MessageHandler.class);

	private MqttAsyncClient publisherClient;
	private MqttAsyncClient listenerClient;
	private Object ctx;
	private HashMap<String, RequestHandler> handlers;
	private ObjectMapper mapper = new ObjectMapper();

	private Object keepRunning = new Object();

	public MessageHandler() {
		handlers = new HashMap<String, RequestHandler>();
	}

	public void putHandler(String key, RequestHandler handler) {
		handlers.put(key, handler);
	}

	public void setPublisherClient(MqttAsyncClient client) {
		this.publisherClient = client;
	}

	public MqttAsyncClient getPublisherClient() {
		return this.publisherClient;
	}

	public void setListenerClient(MqttAsyncClient client) {
		this.listenerClient = client;
	}

	public MqttAsyncClient getListenerClient() {
		return this.listenerClient;
	}

	public void setContext(Object ctx) {
		this.ctx = ctx;
	}

	public void waitForCompletion() throws InterruptedException {
		synchronized (keepRunning) {
			keepRunning.wait();
		}
	}

	private static final int MAX_REQUEST_BYTES = 20 * 1024 * 1024; // e.g. 20 MiB
	private static final int LOG_PREVIEW_BYTES = 256;

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {

		byte[] payload = message.getPayload();
		if (payload == null) {
			log.warn("dropping request with no payload on topic {}", topic);
			return;
		}

		int n = Math.min(payload.length, LOG_PREVIEW_BYTES);
		String prefix = new String(payload, 0, n, StandardCharsets.UTF_8);
		log.info(String.format("Received request: %s", prefix));

		if (payload.length > MAX_REQUEST_BYTES) {
			log.warn("dropping oversized request: {} bytes > limit {} on topic {}", payload.length, MAX_REQUEST_BYTES, topic);
			return;
		}

		MqttProperties requestProperties = message.getProperties();
		if (requestProperties == null) {
			log.warn("dropping request with no properties");
			return;
		}

		byte[] correlationData = requestProperties.getCorrelationData();
		if (correlationData == null) {

			String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestProperties);
			log.debug(String.format("Properties:\n%s", json));

			log.warn("dropping request with no correlationData");
			return;
		}

		String correlID = new String(correlationData);
		log.debug(String.format("correlationData: %s", correlID));

		String responseTopic = requestProperties.getResponseTopic();
		if (responseTopic == null) {
			log.warn("dropping request with no responseTopic");
			return;
		}

		if (responseTopic.length() <= 0) {
			log.warn("dropping request with empty responseTopic");
			return;
		}

		log.debug(String.format("responseTopic:   %s", responseTopic));

		Response response = getResponse(responseTopic, message, requestProperties.getUserProperties());
		log.debug(String.format("result:   %s", response));

		MqttMessage responseMessage = getResponseMessage(message, response);

		publisherClient.publish(responseTopic, responseMessage).waitForCompletion();

		if (response.isQuit()) {
			log.debug("quitting");
			synchronized (keepRunning) {
				keepRunning.notify();
			}
			return;
		}
	}

	private Response getResponse(String responseTopic, MqttMessage requestMessage, List<UserProperty> userProperties) {

		byte[] payload = requestMessage.getPayload();

		Response response = null;
		Request request = null;
		try {
			log.debug("decoding message payload");
			String payloadString = new String(payload);
			request = mapper.readValue(payloadString, Request.class);
		} catch (Exception e) {
			response = Response.status(Status.BAD_REQUEST, e.getMessage());
		}

		if (request == null && response == null) {
			response = Response.status(Status.BAD_REQUEST, "missing request");
		} else if (request.getFunction() == null) {
			response = Response.status(Status.BAD_REQUEST, "missing function");
		} else if (request.getFunction().length() <= 0) {
			response = Response.status(Status.BAD_REQUEST, "empty function");
		} else if (response == null) {

			RequestHandler handler = handlers.get(request.getFunction());
			if (handler == null) {
				response = Response.status(Status.BAD_REQUEST, String.format("unexpected function: %s", request.getFunction()));
			} else {
				try {
					log.debug("before handleRequest");
					response = handler.handleRequest(ctx, request.getArgs(), userProperties);
				} catch (ExpiredJwtException e) {
					log.info("ExpiredJwtException");
					response = Response.status(Status.BAD_REQUEST, e.getMessage());
				} catch (RpcStatusException e) {
					log.debug("RPC status exception: {}", e.getStatus());
					response = Response.status(e.getStatus(), e.getMessage());
				} catch (Exception e) {
					log.error("Unhandled exception while handling request", e);
					response = Response.status(Status.INTERNAL_ERROR, e.getMessage());
				}
			}
		}

		log.debug(String.format("returning: %s", response.toString()));
		return response;

	}

	private MqttMessage getResponseMessage(MqttMessage requestMessage, Response response) {

		if (response == null) {
			response = Response.status(Status.INTERNAL_ERROR, "discarding request because response was null");
		}

		byte[] payload = null;
		try {
			payload = mapper.writeValueAsBytes(response.getPayload());
		} catch (Exception e) {
			log.error("Failed to serialize RPC response", e);
			Response fallback = Response.status(Status.INTERNAL_ERROR, "Failed to serialize RPC response");
			try {
				payload = mapper.writeValueAsBytes(fallback.getPayload());
			} catch (Exception fallbackException) {
				log.error("Failed to serialize fallback RPC response", fallbackException);

				// Last-resort hard-coded JSON. Avoids returning null or crashing
				// while trying to report an error about error-reporting.
				String hardcodedFallback = "{\"status\":{\"code\":500,\"message\":\"Failed to serialize RPC response\"}}";
				payload = hardcodedFallback.getBytes(StandardCharsets.UTF_8);
			}
		}

		int qos = 1;
		MqttProperties properties = new MqttProperties();
		properties.setCorrelationData(requestMessage.getProperties().getCorrelationData());

		List<UserProperty> userProperties = properties.getUserProperties();
		Status status = response.getStatus();
		userProperties.add(new UserProperty("status", getStatusAsJson(status)));

		MqttMessage responseMessage = new MqttMessage(payload);
		responseMessage.setProperties(properties);
		responseMessage.setQos(qos);

		try {
			String json = mapper.writer().writeValueAsString(status);
			log.debug(String.format(BuildInfo.toStaticString()));
			log.info(String.format("Sending status:  %s", json));
			log.info(String.format("Sending payload: %s", new String(payload)));
		} catch (Exception e) {
			String message = String.format("error formatting userProperties: %s", userProperties);
			log.info(message);
			response = Response.status(Status.INTERNAL_ERROR, message);
		}
		return responseMessage;
	}

	public String getStatusAsJson(Status status) {
		String json = "";
		try {
			json = mapper.writer().writeValueAsString(status);
		} catch (Exception e) {
			json = "{ \"code\": 500, \"message\": \"internal error\"}";
		}
		return json;
	}

	@Override
	public void disconnected(MqttDisconnectResponse response) {
		log.warn("MQTT disconnected: " + response.getReasonString());
	}

	@Override
	public void mqttErrorOccurred(MqttException exception) {
		log.error("MQTT error: ", exception);
	}

	@Override
	public void connectComplete(boolean reconnect, String serverURI) {
		log.info("MQTT connect complete (reconnect=" + reconnect + ")");
	}
}
