package com.rsmaxwell.mqtt.rpc.response;

import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rsmaxwell.mqtt.rpc.common.Adapter;
import com.rsmaxwell.mqtt.rpc.common.Request;
import com.rsmaxwell.mqtt.rpc.common.Response;
import com.rsmaxwell.mqtt.rpc.common.Status;
import com.rsmaxwell.mqtt.rpc.response.buildinfo.BuildInfo;
import com.rsmaxwell.mqtt.rpc.utilities.BadRequest;
import com.rsmaxwell.mqtt.rpc.utilities.Unauthorised;

import io.jsonwebtoken.ExpiredJwtException;

public class MessageHandler extends Adapter implements MqttCallback {

	private static final Logger log = LogManager.getLogger(MessageHandler.class);

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

	@Override
	public void messageArrived(String topic, MqttMessage requestMessage) throws Exception {
		log.info(String.format("Received request: %s", new String(requestMessage.getPayload())));

		MqttProperties requestProperties = requestMessage.getProperties();
		if (requestProperties == null) {
			log.error("discarding request with no properties");
			return;
		}

		byte[] correlationData = requestProperties.getCorrelationData();
		if (correlationData == null) {

			String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestProperties);
			log.debug(String.format("Properties:\n%s", json));

			log.error("discarding request with no correlationData");
			return;
		}

		String correlID = new String(correlationData);
		log.debug(String.format("correlationData: %s", correlID));

		String responseTopic = requestProperties.getResponseTopic();
		if (responseTopic == null) {
			log.error("discarding request with no responseTopic");
			return;
		}

		if (responseTopic.length() <= 0) {
			log.error("discarding request with empty responseTopic");
			return;
		}

		log.debug(String.format("responseTopic:   %s", responseTopic));

		Response response = getResponse(responseTopic, requestMessage, requestProperties.getUserProperties());
		log.debug(String.format("result:   %s", response));

		MqttMessage responseMessage = getResponseMessage(requestMessage, response);

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
			response = Response.badRequest(e.getMessage());
		}

		if (request == null) {
			response = Response.badRequest("missing request");
		} else if (request.getFunction() == null) {
			response = Response.badRequest("missing function");
		} else if (request.getFunction().length() <= 0) {
			response = Response.badRequest("empty function");
		} else if (response == null) {

			RequestHandler handler = handlers.get(request.getFunction());
			if (handler == null) {
				response = Response.badRequest(String.format("unexpected function: %s", request.getFunction()));
			}

			try {
				log.debug("before handleRequest");
				response = handler.handleRequest(ctx, request.getArgs(), userProperties);
			} catch (ExpiredJwtException e) {
				log.info("ExpiredJwtException");
				response = Response.badRequest(e.getMessage());
			} catch (Unauthorised e) {
				log.debug("Unauthorised");
				response = Response.unauthorized();
			} catch (BadRequest e) {
				log.debug("BadRequest");
				response = Response.badRequest(e.getMessage());
			} catch (Exception e) {
				log.catching(e);
				response = Response.internalError(e.getMessage());
			}
		}

		log.debug(String.format("returning: %s", response.toString()));
		return response;
	}

	private MqttMessage getResponseMessage(MqttMessage requestMessage, Response response) {

		if (response == null) {
			response = Response.internalError("discarding request because response was null");
		}

		log.debug("encoding response");
		byte[] payload = null;
		try {
			payload = mapper.writeValueAsBytes(response.getPayload());
		} catch (Exception e) {
			log.catching(e);
			response = Response.internalError(e.getMessage());
		}

		if (payload == null) {
			String message = "response payload is null";
			log.error(message);
			response = Response.internalError(message);
		}

		int qos = 0;
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
			log.info(String.format(BuildInfo.toStaticString()));
			log.info(String.format("Sending status:  %s", json));
			log.info(String.format("Sending payload: %s", new String(payload)));
		} catch (Exception e) {
			String message = String.format("error formatting userProperties: %s", userProperties);
			log.info(message);
			response = Response.internalError(message);
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
