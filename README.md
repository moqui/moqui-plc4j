# Moqui Apache PLC4J Tool Component

Moqui tool component for Apache PLC4J allows you to communicate directly with your industrial hardware (PLC, PAC, Soft PLC, drives, servo drives, etc...), without having to retrofit it.

This is possible, as PLC4J implements drivers for the most common industrial communication protocols and uses these to access industrial hardware using a shared API.

This component implements the `moqui.device.DeviceServices.run#DeviceRequestInternal` interface defined by **moqui-device**, providing fieldbus connectivity via Apache PLC4X for Read, Write, Subscribe, and Unsubscribe device requests. Set `runServiceName="moqui.plc4j.Plc4jServices.run#Plc4jRequest"` on a `DeviceRequest` to route it through this driver.

To install run (with moqui-framework):

    $ ./gradlew getComponent -Pcomponent=moqui-plc4j

This will add the component to the Moqui runtime/component directory.

## Error handling

Communication failures (timeouts, driver exceptions, non-OK tag response codes) are propagated as
Moqui service errors for all device requests **except** those with `purposeEnumId="DrpLogging"`.
Logging/telemetry requests catch every exception, write a log entry, and continue so that telemetry data collection never blocks the calling thread.

## Integration tests

The test suite requires **ModbusPal2** (headless Modbus TCP simulator, referenced in the
[official PLC4X documentation](https://plc4x.apache.org/plc4x/latest/users/getting-started/virtual-modbus.html)).
Place `ModbusPal2-*.jar` in `runtime/lib/ModbusPal.jar` before running the tests.
