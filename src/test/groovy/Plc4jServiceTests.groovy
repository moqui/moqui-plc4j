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

    def "write then read back via Modbus TCP (round-trip through ModbusPal)"() {
        given: "seeded Modbus device requests and ModbusPal running on port ${port}"
            assert ec.entity.find("moqui.device.DeviceRequest").condition("requestName", "ModbusWriteReference").one()
            assert ec.entity.find("moqui.device.DeviceRequest").condition("requestName", "ModbusWriteControlWord").one()
            assert ec.entity.find("moqui.device.DeviceRequest").condition("requestName", "ModbusReadReference").one()
            assert ec.entity.find("moqui.device.DeviceRequest").condition("requestName", "ModbusReadControlWord").one()
            assert ec.entity.find("moqui.device.DeviceRequest").condition("requestName", "ModbusReadCoil").one()

        when: "writing Reference=300 (UINT, register 2) to ModbusPal"
            ec.service.sync().name("moqui.device.DeviceServices.run#DeviceRequest")
                .parameters([requestName: "ModbusWriteReference"]).disableAuthz().call()

        then: "no Moqui errors on write Reference"
            !ec.message.hasError()

        when: "writing MainControlWord (UINT, register 0) to ModbusPal"
            ec.service.sync().name("moqui.device.DeviceServices.run#DeviceRequest")
                .parameters([requestName: "ModbusWriteControlWord"]).disableAuthz().call()

        then: "no Moqui errors on write MainControlWord"
            !ec.message.hasError()

        when: "shifting parameter 9000 away from 300 so the read will trigger an entity update"
            ec.entity.find("moqui.math.Parameter").condition("parameterId", "9000")
                .useCache(false).one().set("numericValue", 999.0G).update()

        and: "reading Reference (FC3, register 2)"
            ec.service.sync().name("moqui.device.DeviceServices.run#DeviceRequest")
                .parameters([requestName: "ModbusReadReference"]).disableAuthz().call()

        then: "no Moqui errors on read Reference"
            !ec.message.hasError()

        and: "Reference round-trips as UINT16 through Modbus (register 2 → 300)"
            def reference = ec.entity.find("moqui.math.Parameter").condition("parameterId", "9000").useCache(false).one()
            reference.numericValue == 300.0G

        when: "reading MainControlWord (FC3, register 0)"
            ec.service.sync().name("moqui.device.DeviceServices.run#DeviceRequest")
                .parameters([requestName: "ModbusReadControlWord"]).disableAuthz().call()

        then: "no Moqui errors on read MainControlWord"
            !ec.message.hasError()

        and: "MainControlWord round-trips as UINT16 through Modbus (register 0 → bit pattern)"
            def mainControlWord = ec.entity.find("moqui.math.Parameter").condition("parameterId", "9002").useCache(false).one()
            mainControlWord.parameterEnumId == '1000010001101111'

        when: "reading coil (FC1): Fault ← coil 0"
            ec.service.sync().name("moqui.device.DeviceServices.run#DeviceRequest")
                .parameters([requestName: "ModbusReadCoil"]).disableAuthz().call()

        then: "no Moqui errors on coil read"
            !ec.message.hasError()

        and: "Fault coil reads as active (coil 0 = 1 in ModbusPal initial state)"
            def fault = ec.entity.find("moqui.math.Parameter").condition("parameterId", "9004").useCache(false).one()
            fault.symbolicValue == 'Y'
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
