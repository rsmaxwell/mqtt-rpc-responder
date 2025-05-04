package com.rsmaxwell.mqtt.rpc.response;

import java.net.HttpURLConnection;
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
import com.rsmaxwell.mqtt.rpc.common.Result;
import com.rsmaxwell.mqtt.rpc.common.Status;
import com.rsmaxwell.mqtt.rpc.response.buildinfo.BuildInfo;
import com.rsmaxwell.mqtt.rpc.utilities.BadRequest;
import com.rsmaxwell.mqtt.rpc.utilities.Unauthorised;

import io.jsonwebtoken.ExpiredJwtException;

public class MessageHandler extends Adapter implements MqttCallback {

	private static final Logger log = LogManager.getLogger(MessageHandler.class);

	private MqttAsyncClient client;
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

	public void setClient(MqttAsyncClient client) {
		this.client = client;
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

		Result result = getResult(responseTopic, requestMessage, requestProperties.getUserProperties());
		log.debug(String.format("result:   %s", result));

		MqttMessage responseMessage = getResponseMessage(requestMessage, result);

		client.publish(responseTopic, responseMessage).waitForCompletion();

		if (result.isQuit()) {
			log.debug("quitting");
			synchronized (keepRunning) {
				keepRunning.notify();
			}
			return;
		}
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

	private Result getResult(String responseTopic, MqttMessage requestMessage, List<UserProperty> userProperties) {
		Result result = null;

		String payload = new String(requestMessage.getPayload());

		Request request = null;
		try {
			log.debug("decoding message payload");
			request = mapper.readValue(payload, Request.class);
		} catch (Exception e) {
			return Result.badRequest(e.getMessage());
		}

		if (request == null) {
			return Result.badRequest("missing request");
		}

		if (request.getFunction() == null) {
			return Result.badRequest("missing function");
		}

		if (request.getFunction().length() <= 0) {
			return Result.badRequest("empty function");
		}

		RequestHandler handler = handlers.get(request.getFunction());
		if (handler == null) {
			return Result.badRequest(String.format("unexpected function: %s", request.getFunction()));
		}

		try {
			log.debug("before handleRequest");
			result = handler.handleRequest(ctx, request.getArgs(), userProperties);
		} catch (ExpiredJwtException e) {
			log.info("ExpiredJwtException");
		} catch (Unauthorised e) {
			log.debug("Unauthorised");
			return Result.unauthorised();
		} catch (BadRequest e) {
			log.debug("BadRequest");
			return Result.badRequest(e.getMessage());
		} catch (Exception e) {
			log.debug("Exception");
			log.catching(e);
			return Result.internalError(e.getMessage());
		}

		if (result == null) {
			return Result.badRequest("result is null");
		}

		log.debug(String.format("returning: %s", result.toString()));
		return result;
	}

	private MqttMessage getResponseMessage(MqttMessage requestMessage, Result result) {

		Response response = result.getResponse();
		if (response == null) {
			response = Response.internalError("discarding request because response was null");
		}

		log.debug("encoding response");
		byte[] body = null;
		try {
			body = mapper.writeValueAsBytes(response);
		} catch (Exception e) {
			log.catching(e);
			String message = e.getMessage();
			body = message.getBytes();
		}

		if (body == null) {
			String message = "response body is null";
			log.error(message);
			body = message.getBytes();
		}

		int qos = 0;
		MqttProperties responseProperties = new MqttProperties();
		responseProperties.setCorrelationData(requestMessage.getProperties().getCorrelationData());

		MqttMessage responseMessage = new MqttMessage(body);
		responseMessage.setProperties(responseProperties);
		responseMessage.setQos(qos);

		List<UserProperty> userProperties = responseMessage.getProperties().getUserProperties();
		userProperties.add(new UserProperty("status", getStatusAsJson(response)));

		try {
			String json = mapper.writer().writeValueAsString(userProperties);
			log.info(String.format(BuildInfo.toStaticString()));
			log.info(String.format("Sending status: %s", json));
			log.info(String.format("Sending reply: %s", new String(body)));
		} catch (Exception e) {
			log.info(String.format("error formatting userProperties: %s", userProperties));
		}
		return responseMessage;
	}

	public String getStatusAsJson(Response response) {

		int code = 0;
		String message = "ok";
		try {
			code = response.getInteger("code");
			message = response.getString("message");
		} catch (Exception e) {
			code = HttpURLConnection.HTTP_INTERNAL_ERROR;
			message = e.getMessage();
		}

		String json = "";
		try {
			json = mapper.writer().writeValueAsString(new Status(code, message));
		} catch (Exception e) {
			json = "{ \"code\": 500, \"message\": \"internal error\"}";
		}

		return json;
	}
}
