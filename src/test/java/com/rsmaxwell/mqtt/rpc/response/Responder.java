package com.rsmaxwell.mqtt.rpc.response;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttClientPersistence;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MqttDefaultFilePersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttSubscription;

import com.rsmaxwell.mqtt.rpc.response.handlers.CalculatorHandler;
import com.rsmaxwell.mqtt.rpc.response.handlers.GetPagesHandler;
import com.rsmaxwell.mqtt.rpc.response.handlers.QuitHandler;

public class Responder {

	private static final Logger logger = LogManager.getLogger(Responder.class);

	static String clientID_publisher = "publisher";
	static String clientID_listener = "listener";
	static String requestTopic = "request";

	static int qos = 0;

	static MessageHandler messageHandler;

	static {
		messageHandler = new MessageHandler();
		messageHandler.putHandler("calculator", new CalculatorHandler());
		messageHandler.putHandler("getPages", new GetPagesHandler());
		messageHandler.putHandler("quit", new QuitHandler());
	}

	static Option createOption(String shortName, String longName, String argName, String description, boolean required) {
		return Option.builder(shortName).longOpt(longName).argName(argName).desc(description).hasArg().required(required).build();
	}

	public static void main(String[] args) throws Exception {

		logger.info("mqtt-rcp Responder");

		Options options = new Options();
		Option serverOption = createOption("s", "server", "mqtt server", "URL of MQTT server", false);
		Option usernameOption = createOption("u", "username", "Username", "Username for the MQTT server", true);
		Option passwordOption = createOption("p", "password", "Password", "Password for the MQTT server", true);
		options.addOption(serverOption).addOption(usernameOption).addOption(passwordOption);

		CommandLineParser commandLineParser = new DefaultParser();
		CommandLine commandLine = commandLineParser.parse(options, args);
		String server = commandLine.hasOption("h") ? commandLine.getOptionValue(serverOption) : "tcp://127.0.0.1:1883";
		String username = commandLine.getOptionValue(usernameOption);
		String password = commandLine.getOptionValue(passwordOption);

		MqttClientPersistence persistence = new MqttDefaultFilePersistence();

		MqttAsyncClient client_responder = new MqttAsyncClient(server, clientID_publisher, persistence);
		MqttAsyncClient client_subscriber = new MqttAsyncClient(server, clientID_listener, persistence);

		messageHandler.setPublisherClient(client_responder);
		client_subscriber.setCallback(messageHandler);

		logger.info(String.format("Connecting to broker: %s as '%s'", server, clientID_publisher));
		MqttConnectionOptions connOpts_responder = new MqttConnectionOptions();
		connOpts_responder.setUserName(username);
		connOpts_responder.setPassword(password.getBytes());
		connOpts_responder.setCleanStart(true);
		try {
			client_responder.connect(connOpts_responder).waitForCompletion();
		} catch (MqttException e) {
			logger.info(String.format("Could not connect to the MQTT Broker at: %s", server));
			return;
		} catch (Exception e) {
			logger.error("%s: %s", e.getClass().getSimpleName(), e.getMessage());
			return;
		}

		logger.info(String.format("Connecting to broker: %s as '%s'", server, clientID_listener));
		MqttConnectionOptions connOpts_subscriber = new MqttConnectionOptions();
		connOpts_subscriber.setUserName(username);
		connOpts_subscriber.setPassword(password.getBytes());
		connOpts_subscriber.setCleanStart(true);
		client_subscriber.connect(connOpts_subscriber).waitForCompletion();

		logger.info(String.format("subscribing to: %s", requestTopic));
		MqttSubscription subscription = new MqttSubscription(requestTopic);
		client_subscriber.subscribe(subscription).waitForCompletion();

		// Wait till quit request received
		messageHandler.waitForCompletion();

		logger.info("disconnect");
		client_responder.disconnect().waitForCompletion();
		client_subscriber.disconnect().waitForCompletion();

		logger.info("exiting");
	}
}
