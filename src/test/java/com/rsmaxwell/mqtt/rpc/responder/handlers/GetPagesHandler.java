package com.rsmaxwell.mqtt.rpc.responder.handlers;

import java.util.List;
import java.util.Map;

import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rsmaxwell.mqtt.rpc.common.Response;
import com.rsmaxwell.mqtt.rpc.responder.RequestHandler;

public class GetPagesHandler extends RequestHandler {

	private static final Logger logger = LoggerFactory.getLogger(GetPagesHandler.class);

	@Override
	public Response handleRequest(Object ctx, Map<String, Object> args, List<UserProperty> userProperties) throws Exception {
		logger.trace("Entering method");
		return Response.success("[ 'one', 'two', 'three' ]");
	}
}
