package com.rsmaxwell.mqtt.rpc.response;

import java.util.Map;

import com.rsmaxwell.mqtt.rpc.common.Response;

public abstract class RequestHandler {

	public abstract Response handleRequest(Object ctx, Map<String, Object> args) throws Exception;
}
