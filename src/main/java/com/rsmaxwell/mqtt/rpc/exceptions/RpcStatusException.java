package com.rsmaxwell.mqtt.rpc.exceptions;

import com.rsmaxwell.mqtt.rpc.common.Status;

public class RpcStatusException extends Exception {

	private static final long serialVersionUID = 1L;

	private final Status status;

	public RpcStatusException(Status status, String message) {
		super(message);
		this.status = status;
	}

	public RpcStatusException(Status status, String message, Throwable cause) {
		super(message, cause);
		this.status = status;
	}

	public Status getStatus() {
		return status;
	}

	public static RpcStatusException badRequest(String message) {
		return new RpcStatusException(Status.BAD_REQUEST, message);
	}

	public static RpcStatusException unauthorized(String message) {
		return new RpcStatusException(Status.UNAUTHORIZED, message);
	}

	public static RpcStatusException forbidden(String message) {
		return new RpcStatusException(Status.FORBIDDEN, message);
	}

	public static RpcStatusException conflict(String message) {
		return new RpcStatusException(Status.CONFLICT, message);
	}

	public static RpcStatusException internalError(String message) {
		return new RpcStatusException(Status.INTERNAL_ERROR, message);
	}

	public static RpcStatusException notFound(String message) {
		return new RpcStatusException(Status.NOT_FOUND, message);
	}
}
