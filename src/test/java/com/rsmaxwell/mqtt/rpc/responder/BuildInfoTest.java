package com.rsmaxwell.mqtt.rpc.responder;

import com.rsmaxwell.mqtt.rpc.common.buildinfo.BuildInfo;
import org.junit.jupiter.api.Test;

class BuildInfoTest {

    @Test
    void printsBuildInfo() throws Exception {
        BuildInfo info = new BuildInfo();
        info.printAll();
    }
}