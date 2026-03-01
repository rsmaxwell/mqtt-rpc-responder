package com.rsmaxwell.mqtt.rpc.utilities;

public class Conflict extends StatusException {

	public Conflict(int code) {
		super(code);
	}

	public Conflict(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public Conflict(String message, Throwable cause) {
		super(message, cause);
	}

	public Conflict(String message) {
		super(message);
	}

	public Conflict(Throwable cause) {
		super(cause);
	}
}
