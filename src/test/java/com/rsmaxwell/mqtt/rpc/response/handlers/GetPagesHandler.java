package com.rsmaxwell.mqtt.rpc.response.handlers;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rsmaxwell.mqtt.rpc.common.Response;
import com.rsmaxwell.mqtt.rpc.response.RequestHandler;

public class GetPagesHandler extends RequestHandler {

	private static final Logger logger = LogManager.getLogger(GetPagesHandler.class);

	@Override
	public Response handleRequest(Object ctx, Map<String, Object> args) throws Exception {
		logger.traceEntry();
		return Response.success("[ 'one', 'two', 'three' ]");
	}
}
