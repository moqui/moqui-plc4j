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
