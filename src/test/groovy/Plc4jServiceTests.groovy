/*
 * This software is in the public domain under CC0 1.0 Universal plus a
 * Grant of Patent License.
 *
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

import java.net.Socket
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit

import org.moqui.Moqui
import org.moqui.context.ExecutionContext
import org.moqui.plc4j.Plc4jToolFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Stepwise
class Plc4jServiceTests extends Specification {
    @Shared protected static final Logger logger = LoggerFactory.getLogger(Plc4jServiceTests)

    @Shared ExecutionContext ec
    @Shared Plc4jToolFactory plc4jTool

    @Shared Process modbusPal
    @Shared String host = "127.0.0.1"
    @Shared int port = 1508
    @Shared String moqui_runtime = System.getProperty("moqui.runtime")
    @Shared File xmppFile = new File("${moqui_runtime}/component/moqui-plc4j/data/modbuspal/modbuspal.xmpp")
    @Shared File modbusPalJar = new File("${moqui_runtime}/lib/ModbusPal.jar")

    def setupSpec() {
        assert modbusPalJar.exists(): "Set the absolute path to ModbusPal.jar file"
        assert xmppFile.exists(): "Set the absolute path to modbuspal.xmpp file"
        // Start ModbusPal headless
        String javaBin = new File(System.getProperty("java.home"), "bin/java").absolutePath
        List<String> cmd = [
            javaBin,
            "-jar", modbusPalJar.absolutePath,
            "-ng",
            "-f", xmppFile.absolutePath,
            "-tp", String.valueOf(port)
        ].collect { it.toString() }
        ProcessBuilder processBuilder = new ProcessBuilder(cmd).redirectErrorStream(true)
        Map<String, String> env = processBuilder.environment()
        String home = env.get("HOME")
        env.clear()
        if (home) env.put("HOME", home)
        env.put("PATH", new File(System.getProperty("java.home"), "bin").absolutePath + ":/usr/bin:/bin")
        env.put("LANG", "C.UTF-8")
        modbusPal = processBuilder.start()

        StringBuilder modbusPalOut = new StringBuilder()
        Thread.startDaemon("modbuspal-logger") {
            try {
                modbusPal.inputStream.withReader { r ->
                    r.eachLine { line ->
                        modbusPalOut.append(line).append('\n')
                        logger.info("[ModbusPal] {}", line)
                    }
                }
            } catch (IOException ignored) {
                // stream closed during teardown – ignore
            }
        }
    
        sleep(500)
        if (!modbusPal.isAlive()) {
            int code = modbusPal.exitValue()
            throw new AssertionError("ModbusPal exited immediately (code ${code}). Output:\n${modbusPalOut}")
        }

        // Wait until TCP port listens (up to 60s)
        try {
            waitForPortOrFail(host, port, Duration.ofSeconds(120))
        } catch (AssertionError e) {
            def alive = modbusPal?.isAlive()
            def exitCode = alive ? "(still running)" : "exitCode=${modbusPal?.exitValue()}"
            throw new AssertionError("Timed out waiting for ${host}:${port} to accept connections; ModbusPal ${exitCode}\n--- ModbusPal output ---\n${modbusPalOut}\n------------------------", e)
        }

        System.setProperty("plc4j_cached_connection", "false")
        // With moqui.init.static=true, checkEmptyDb() is not called automatically (it's only
        // called via Moqui.dynamicInit() in the servlet path). Call it explicitly so seed/demo
        // data is loaded into a fresh H2 database before the tests run.
        Moqui.getExecutionContextFactory().checkEmptyDb()
        ec = Moqui.getExecutionContext()
        ec.user.loginUser("john.doe", "moqui")
        plc4jTool = ec.getTool(Plc4jToolFactory.TOOL_NAME, Plc4jToolFactory.class)
    }

    def cleanupSpec() {
        try {
            if (modbusPal != null) {
                modbusPal.destroy()
                modbusPal.waitFor(3, TimeUnit.SECONDS)
            }
        } catch (Throwable ignore) {}

        if (ec != null) ec.destroy()
    }

    def setup() {
        ec.artifactExecution.disableAuthz()
        if (!ec.user.getUserId()) ec.user.loginUser("john.doe", "moqui")
    }

    def cleanup() {
        ec.artifactExecution.enableAuthz()
        ec.user.logoutUser()
    }

    def "write then read back cyclic data via device requests (round-trip through Modbus)"() {
        given: "seeded device requests and parameters exist"
            def writeReq = ec.entity.find("moqui.device.DeviceRequest")
                .condition("requestName", "SimulatedWriteCyclicData").one()
            def readReq = ec.entity.find("moqui.device.DeviceRequest")
                .condition("requestName", "SimulatedBackRead").useCache(false).one()
            String writeReqName = writeReq?.getString("requestName")
            String readReqName = readReq?.getString("requestName")
            logger.info("Found writeReq ${writeReqName}, readReq ${readReqName}")
            assert writeReq && readReq

        when: "calling write request service to push values to Modbus"
            ec.service.sync().name("moqui.device.DeviceServices.run#DeviceRequest")
                .parameters([requestName: writeReqName]).disableAuthz().call()

        then: "no Moqui errors on write"
            !ec.message.hasError()

        when: "calling read request service to pull current live values from Modbus"
            ec.service.sync().name("moqui.device.DeviceServices.run#DeviceRequest")
                .parameters([requestName: readReqName]).disableAuthz().call()

        then: "no Moqui errors on read"
            !ec.message.hasError()

        and: "Parameters now reflect exactly what we wrote (verified through the device)"
            def reference = ec.entity.find("moqui.math.Parameter").condition("parameterId", "9000").one()
            def mainControlWord = ec.entity.find("moqui.math.Parameter").condition("parameterId", "9002").one()
            reference.numericValue == 300.0
            mainControlWord.parameterEnumId == '1000010001101111'
    }

    def "read cyclic data via a device request (through Modbus)"() {
        given: "seeded device request and parameters exist"
            def readReq = ec.entity.find("moqui.device.DeviceRequest")
                .condition("requestName", "SimulatedReadCyclicData").useCache(false).one()
            String readReqName = readReq?.getString("requestName")
            logger.info("Found readReq ${readReqName}")
            assert readReq

        when: "calling read request service to collect values from Modbus"
            ec.service.sync().name("moqui.device.DeviceServices.run#DeviceRequest")
                .parameters([requestName: readReqName]).disableAuthz().call()

        then: "no Moqui errors on read"
            !ec.message.hasError()

        and: "Parameters now reflect exactly what we read"
            def feedback = ec.entity.find("moqui.math.Parameter").condition("parameterId", "9001").one()
            def mainStatusWord = ec.entity.find("moqui.math.Parameter").condition("parameterId", "9003").one()
            feedback.numericValue == 300.03
            mainStatusWord.parameterEnumId == '1000010001101001'
    }

    // helpers
    private static void waitForPortOrFail(String host, int port, Duration timeout) {
        def deadline = Instant.now().plus(timeout)
        Throwable last
        while (Instant.now().isBefore(deadline)) {
            try {
                new Socket(host, port).withCloseable { /* success */ }
                return
            } catch (Throwable t) {
                last = t
                Thread.sleep(250)
            }
        }
        throw new AssertionError("Timed out waiting for $host:$port", last)
    }
}
