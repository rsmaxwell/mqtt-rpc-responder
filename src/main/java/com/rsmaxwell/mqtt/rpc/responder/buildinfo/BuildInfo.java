package com.rsmaxwell.mqtt.rpc.responder.buildinfo;

import com.rsmaxwell.mqtt.rpc.common.buildinfo.AbstractBuildInfo;

public class BuildInfo extends AbstractBuildInfo {

	public BuildInfo() {
		name = "mqtt-rpc-response";
		version = "$VERSION";
		buildID = "$BUILD_ID";
		builddate = "$TIMESTAMP";
		gitCommit = "$GIT_COMMIT";
		gitBranch = "$GIT_BRANCH";
		gitURL = "$GIT_URL";
	}

	public static String toStaticString() {
		BuildInfo info = new BuildInfo();
		return info.toString();
	}

	@Override
	public void printAll() throws Exception {
		System.out.println(toStaticString());
	}
}
