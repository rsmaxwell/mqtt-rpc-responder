package com.rsmaxwell.mqtt.rpc.response.handlers;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;

import com.rsmaxwell.mqtt.rpc.common.Result;
import com.rsmaxwell.mqtt.rpc.common.Utilities;
import com.rsmaxwell.mqtt.rpc.response.RequestHandler;

public class CalculatorHandler extends RequestHandler {

	private static final Logger logger = LogManager.getLogger(CalculatorHandler.class);

	@Override
	public Result handleRequest(Object ctx, Map<String, Object> args, List<UserProperty> userProperties) throws Exception {
		logger.traceEntry();

		try {
			String operation = Utilities.getString(args, "operation");
			int param1 = Utilities.getInteger(args, "param1");
			int param2 = Utilities.getInteger(args, "param2");

			int value = 0;

			switch (operation) {
			case "add":
				value = param1 + param2;
				break;
			case "mul":
				value = param1 * param2;
				break;
			case "div":
				value = param1 / param2;
				break;
			case "sub":
				value = param1 - param2;
				break;
			default:
				String text = String.format("Unexpected operation: %s", operation);
				logger.info(text);
				throw new Exception(text);
			}

			return Result.success(value);
		} catch (ArithmeticException e) {
			logger.debug(String.format("%s: %s", e.getClass().getSimpleName(), e.getMessage()));
			return Result.badRequest(e.getMessage());
		} catch (Exception e) {
			logger.catching(e);
			return Result.badRequest(e.getMessage());
		}
	}
}
