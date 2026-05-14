# Moqui Apache PLC4J Tool Component

Moqui tool component for Apache PLC4J allows you to communicate directly with your industrial hardware (PLC, PAC, Soft PLC, drives, servo drives, etc...), without having to retrofit it.

This is possible, as PLC4J implements drivers for the most common industrial communication protocols and uses these to access industrial hardware using a shared API.

This component implements the `moqui.device.DeviceServices.run#DeviceRequestInternal` interface defined by **moqui-device**, providing fieldbus connectivity via Apache PLC4X for Read, Write, Subscribe, and Unsubscribe device requests. Set `runServiceName="moqui.plc4j.Plc4jServices.run#Plc4jRequest"` on a `DeviceRequest` to route it through this driver.

To install run (with moqui-framework):

    $ ./gradlew getComponent -Pcomponent=moqui-plc4j

This will add the component to the Moqui runtime/component directory.

The Apache PLC4J and dependent JAR files are added to the lib directory when the build is run for this component, which is
designed to be done from the Moqui build (ie from the moqui root directory) along with all other component builds.

To use just install this component. The configuration for the ToolFactory is already in place in the
MoquiConf.xml included in this component and will be merged with the main configuration at runtime.

## Tag addressing

PLC4X uses **1-based logical addresses** for all Modbus tag types. The logical address is what you
write in the `DeviceRequestItem.query` field; the driver subtracts 1 internally to obtain the
0-based wire address sent in the Modbus PDU.

| Query | Logical address | Wire address (PDU) | ModbusPal register/coil |
|---|---|---|---|
| `holding-register:1:UINT` | 1 | 0 | register[0] |
| `holding-register:2:UINT` | 2 | 1 | register[1] |
| `coil:1:BOOL` | 1 | 0 | coil[0] |

Using address 0 (e.g. `holding-register:0`) is invalid — PLC4X rejects logical address 0 at
tag-parse time and the resulting error is surfaced to the Moqui message context.

## Error handling

Communication failures (timeouts, driver exceptions, non-OK tag response codes) are propagated as
Moqui service errors for all device requests **except** those with `purposeEnumId="DrpLogging"`.
Logging/telemetry requests catch every exception, write a log entry, and continue so that telemetry
collection never blocks the calling thread.

## Integration tests

The test suite requires **ModbusPal2** (headless Modbus TCP simulator, referenced in the
[official PLC4X documentation](https://plc4x.apache.org/plc4x/latest/users/getting-started/virtual-modbus.html)).
Place `ModbusPal2-*.jar` in `runtime/lib/ModbusPal.jar` before running the tests.
