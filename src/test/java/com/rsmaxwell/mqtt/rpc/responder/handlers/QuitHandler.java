package com.rsmaxwell.mqtt.rpc.responder.handlers;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;

import com.rsmaxwell.mqtt.rpc.common.Response;
import com.rsmaxwell.mqtt.rpc.responder.RequestHandler;

public class QuitHandler extends RequestHandler {

	private static final Logger logger = LogManager.getLogger(QuitHandler.class);

	@Override
	public Response handleRequest(Object ctx, Map<String, Object> args, List<UserProperty> userProperties) throws Exception {
		logger.traceEntry();
		return Response.quit();
	}
}
