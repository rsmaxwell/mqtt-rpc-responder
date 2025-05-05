package com.rsmaxwell.mqtt.rpc.response;

import java.util.List;
import java.util.Map;

import org.eclipse.paho.mqttv5.common.packet.UserProperty;

import com.rsmaxwell.mqtt.rpc.common.Response;

public abstract class RequestHandler {

	public abstract Response handleRequest(Object ctx, Map<String, Object> args, List<UserProperty> userProperties) throws Exception;
}
