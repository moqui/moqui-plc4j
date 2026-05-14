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
package org.moqui.plc4j

import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import java.util.concurrent.TimeUnit

import org.moqui.Moqui
import org.moqui.context.ExecutionContext
import org.moqui.entity.EntityValue
import org.moqui.entity.EntityList
import org.moqui.BaseException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.plc4x.java.api.PlcConnection
import org.apache.plc4x.java.api.messages.PlcReadRequest
import org.apache.plc4x.java.api.messages.PlcReadResponse
import org.apache.plc4x.java.api.messages.PlcWriteRequest
import org.apache.plc4x.java.api.messages.PlcWriteResponse
import org.apache.plc4x.java.api.types.PlcResponseCode
import org.apache.plc4x.java.api.messages.PlcSubscriptionRequest
import org.apache.plc4x.java.api.messages.PlcSubscriptionResponse
import org.apache.plc4x.java.api.messages.PlcSubscriptionEvent
import org.apache.plc4x.java.api.model.PlcSubscriptionHandle

class FieldbusClient implements Closeable, AutoCloseable {
    private final static Logger logger = LoggerFactory.getLogger(FieldbusClient.class)
    private final ExecutionContext ec
    private final Plc4jToolFactory tool
    private final PlcConnection connection

    FieldbusClient(ExecutionContext ec, String connectionString) {
        this.ec = ec
        this.tool = ec.getTool(Plc4jToolFactory.TOOL_NAME, Plc4jToolFactory.class)
        this.connection = tool.getConnection(connectionString)
    }

    FieldbusClient(ExecutionContext ec, String connectionString, String username, String password) {
        this.ec = ec
        this.tool = ec.getTool(Plc4jToolFactory.TOOL_NAME, Plc4jToolFactory.class)
        this.connection = tool.getConnection(connectionString, username, password)
    }

    void read(final EntityValue request, final List bulkRequestItemList, final List requestItemList) {
        if ("DrtRead" != request.requestTypeEnumId)
            throw new BaseException("The device request with name ${request.requestName} is not a read request.")
        // Check if this connection support reading data.
        if (!connection.getMetadata().isReadSupported())
            throw new BaseException("This connection for device request with name ${request.requestName} doesn't support reading.")

        // build a new read request
        final PlcReadRequest.Builder builder = connection.readRequestBuilder()
        // Set the query for bulk items
        if (request.query && bulkRequestItemList) {
            builder.addTagAddress(request.requestName, request.query)
        }
        // Set queries for each individual item
        for (EntityValue requestItem in requestItemList) {
            if (requestItem.query) builder.addTagAddress(requestItem.parameterId, requestItem.query)
        }
        // run request and get response
        int connectionTimeout = request.timeout ? request.timeout as int : Integer.getInteger("plc4j_default_connection_timeout", 5000)
        final PlcReadRequest readRequest = builder.build()
        final PlcReadResponse readResponse
        boolean logging = ('DrpLogging' == request.purposeEnumId)
        try {
            readResponse = readRequest.execute().get(connectionTimeout, TimeUnit.MILLISECONDS)
        } catch (TimeoutException e) {
            String msg = "Timeout reading device request ${request.requestName} after ${connectionTimeout}ms"
            if (logging) { logger.warn(msg); return }
            throw new BaseException(msg, e)
        } catch (ExecutionException e) {
            String msg = "Execution error reading device request ${request.requestName}: ${e.cause?.message ?: e.message}"
            if (logging) { logger.error(msg, e); return }
            throw new BaseException(msg, e)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt()
            throw new BaseException("Interrupted reading device request ${request.requestName}", e)
        }

        // response processing
        List updatedParameterList = []
        EntityValue updatedParameter
        for (String tagName in readResponse.getTagNames()) {
            if (readResponse.getResponseCode(tagName) != PlcResponseCode.OK) {
                String msg = "Error reading tag ${tagName} for device request ${request.requestName}: ${readResponse.getResponseCode(tagName).name()}"
                if (logging) { logger.error(msg); continue }
                throw new BaseException(msg)
            }

            if (tagName == request.requestName) {
                // multiple values per tagName; bulk items read
                int numValues = readResponse.getNumberOfValues(request.requestName)
                for (int i = 0; i < numValues; i++) {
                    EntityValue bulkRequestItem = bulkRequestItemList[i]
                    updatedParameter = Plc4jUtil.processInputValue(bulkRequestItem, readResponse.getPlcValue(tagName).getIndex(i))
                    if (request.purposeEnumId == 'DrpLogging') updatedParameter = buildParameterLog(updatedParameter)
                    updatedParameterList.add(updatedParameter)
                }
            } else {
                // single value per tagName
                EntityValue requestItem = requestItemList.find { item -> tagName == item.parameterId }
                if (!requestItem) { logger.warn("No request item for tag ${tagName} in device request ${request.requestName}, skipping"); continue }
                updatedParameter = Plc4jUtil.processInputValue(requestItem, readResponse.getPlcValue(tagName))
                if (request.purposeEnumId == 'DrpLogging') updatedParameter = buildParameterLog(updatedParameter)
                updatedParameterList.add(updatedParameter)
            }
        }

        Plc4jUtil.createOrUpdateBulk(updatedParameterList)
    }

    void write(final EntityValue request, final List bulkRequestItemList, final List requestItemList) {
        if ("DrtWrite" != request.requestTypeEnumId)
            throw new BaseException("The device request with name ${request.requestName} is not a write request.")
        // Check if this connection support writing data.
        if (!connection.getMetadata().isWriteSupported())
            throw new BaseException("This connection for device request with name ${request.requestName} doesn't support writing.")

        // build a new write request
        final PlcWriteRequest.Builder builder = connection.writeRequestBuilder()
        // bulk multi-value per tagName
        if (request.query && bulkRequestItemList) {
            List<Object> physicalValueList = []
            for (EntityValue bulkRequestItem in bulkRequestItemList) {
                Object physicalValue = Plc4jUtil.processOutputValue(bulkRequestItem)
                if (physicalValue != null) physicalValueList.add(physicalValue)
            }
            if (physicalValueList) builder.addTagAddress(request.requestName, request.query, *physicalValueList as Object[])
        }
        // single value per tagName
        for (EntityValue requestItem in requestItemList) {
            Object physicalValue = Plc4jUtil.processOutputValue(requestItem)
            if (physicalValue != null && requestItem.query) builder.addTagAddress(requestItem.parameterId, requestItem.query, physicalValue)
        }
        // run request and get response
        int connectionTimeout = request.timeout ? request.timeout as int : Integer.getInteger("plc4j_default_connection_timeout", 5000)
        final PlcWriteRequest writeRequest = builder.build()
        final PlcWriteResponse writeResponse
        boolean logging = ('DrpLogging' == request.purposeEnumId)
        try {
            writeResponse = writeRequest.execute().get(connectionTimeout, TimeUnit.MILLISECONDS)
        } catch (TimeoutException e) {
            String msg = "Timeout writing device request ${request.requestName} after ${connectionTimeout}ms"
            if (logging) { logger.warn(msg); return }
            throw new BaseException(msg, e)
        } catch (ExecutionException e) {
            String msg = "Execution error writing device request ${request.requestName}: ${e.cause?.message ?: e.message}"
            if (logging) { logger.error(msg, e); return }
            throw new BaseException(msg, e)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt()
            throw new BaseException("Interrupted writing device request ${request.requestName}", e)
        }

        // response processing
        for (String tagName : writeResponse.getTagNames()) {
            if (writeResponse.getResponseCode(tagName) != PlcResponseCode.OK) {
                String msg = "Error writing tag ${tagName} for device request ${request.requestName}: ${writeResponse.getResponseCode(tagName).name()}"
                if (logging) { logger.error(msg); continue }
                throw new BaseException(msg)
            }
        }
    }

    void subscribe(final EntityValue request, final List bulkRequestItemList, final List requestItemList) {
        if ("DrtEvent" != request.requestTypeEnumId
                && "DrtStateChange" != request.requestTypeEnumId
                && "DrtCyclic" != request.requestTypeEnumId)
            throw new BaseException("The device request with name ${request.requestName} is not a subscribe request.")
        if (!connection.getMetadata().isSubscribeSupported())
            throw new BaseException("The connection for device request with name ${request.requestName} doesn't support subscription.")

        // build a new subscription request
        final PlcSubscriptionRequest.Builder builder = connection.subscriptionRequestBuilder()
        // set the query for bulk items
        if (request.query && bulkRequestItemList) {
            if ("DrtEvent" == request.requestTypeEnumId) builder.addEventTagAddress(request.requestName, request.query)
            if ("DrtStateChange" == request.requestTypeEnumId) builder.addChangeOfStateTagAddress(request.requestName, request.query)
            if ("DrtCyclic" == request.requestTypeEnumId) builder.addCyclicTagAddress(request.requestName, request.query, Duration.ofMillis(request.pollingInterval))
        }
        // set queries for each individual item
        for (EntityValue requestItem in requestItemList) {
            if (!requestItem.query) continue
            if ("DrtEvent" == request.requestTypeEnumId) builder.addEventTagAddress(requestItem.parameterId, requestItem.query)
            if ("DrtStateChange" == request.requestTypeEnumId) builder.addChangeOfStateTagAddress(requestItem.parameterId, requestItem.query)
            if ("DrtCyclic" == request.requestTypeEnumId) builder.addCyclicTagAddress(requestItem.parameterId, requestItem.query, Duration.ofMillis(request.pollingInterval))
        }
        // run request and get response
        int connectionTimeout = request.timeout ? request.timeout as int : Integer.getInteger("plc4j_default_connection_timeout", 5000)
        final PlcSubscriptionRequest subscriptionRequest = builder.build()
        final PlcSubscriptionResponse subscriptionResponse
        boolean logging = ('DrpLogging' == request.purposeEnumId)
        try {
            subscriptionResponse = subscriptionRequest.execute().get(connectionTimeout, TimeUnit.MILLISECONDS)
        } catch (TimeoutException e) {
            String msg = "Timeout subscribing device request ${request.requestName} after ${connectionTimeout}ms"
            if (logging) { logger.warn(msg); return }
            throw new BaseException(msg, e)
        } catch (ExecutionException e) {
            String msg = "Execution error subscribing device request ${request.requestName}: ${e.cause?.message ?: e.message}"
            if (logging) { logger.error(msg, e); return }
            throw new BaseException(msg, e)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt()
            throw new BaseException("Interrupted subscribing device request ${request.requestName}", e)
        }

        for (String responseTagName in subscriptionResponse.getTagNames()) {
            final PlcSubscriptionHandle subscriptionHandle = subscriptionResponse.getSubscriptionHandle(responseTagName)

            // Errors inside the subscription callback are logged only — the callback runs in a
            // driver-managed thread where throwing would silently swallow the exception anyway.
            subscriptionHandle.register((PlcSubscriptionEvent subscriptionEvent) -> {
                ExecutionContext localEc = Moqui.getExecutionContext()
                try {
                    localEc.artifactExecution.disableAuthz()
                    localEc.user.loginAnonymousIfNoUser()

                    // response processing
                    List updatedParameterList = []
                    EntityValue updatedParameter
                    for (String tagName in subscriptionEvent.getTagNames()) {
                        if (subscriptionEvent.getResponseCode(tagName) != PlcResponseCode.OK) {
                            logger.error("Error receiving tag ${tagName} for device request ${request.requestName}: ${subscriptionEvent.getResponseCode(tagName).name()}")
                            continue
                        }

                        if (tagName == request.requestName) {
                            // multiple values per tagName; bulk items read
                            int numValues = subscriptionEvent.getNumberOfValues(request.requestName)
                            for (int i = 0; i < numValues; i++) {
                                EntityValue bulkRequestItem = bulkRequestItemList[i]
                                updatedParameter = Plc4jUtil.processInputValue(bulkRequestItem, subscriptionEvent.getPlcValue(tagName).getIndex(i))
                                if (request.purposeEnumId == 'DrpLogging') updatedParameter = buildParameterLog(updatedParameter)
                                updatedParameterList.add(updatedParameter)
                            }
                        } else {
                            // single value per tagName
                            EntityValue requestItem = requestItemList.find { item -> tagName.equals(item.parameterId) }
                            if (!requestItem) { logger.warn("No request item for tag ${tagName} in subscription ${request.requestName}, skipping"); continue }
                            updatedParameter = Plc4jUtil.processInputValue(requestItem, subscriptionEvent.getPlcValue(tagName))
                            if (request.purposeEnumId == 'DrpLogging') updatedParameter = buildParameterLog(updatedParameter)
                            updatedParameterList.add(updatedParameter)
                        }
                    }

                    Plc4jUtil.createOrUpdateBulk(updatedParameterList)
                } finally {
                    localEc.destroy()
                }

            })

            tool.putSubscription(responseTagName, subscriptionHandle)
            logger.info("Subscription handle ${subscriptionHandle} registered.")
        }                    
    }

    void unsubscribe(final EntityValue request, final List requestItemList) {
        if ("DrtUnsubscribe" != request.requestTypeEnumId)
            throw new BaseException("The device request with name ${request.requestName} is not an unsubscribe request.")
        if (!connection.getMetadata().isSubscribeSupported())
            throw new BaseException("The connection for device request with name ${request.requestName} doesn't support subscription.")

        List<String> names = []
        if (request.requestName) names.add(request.requestName)
        if (requestItemList) names.addAll(requestItemList*.parameterId)

        int connectionTimeout = request.timeout ? request.timeout as int : Integer.getInteger("plc4j_default_connection_timeout", 5000)
        int success = tool.unsubscribe(names, connection, connectionTimeout)
        logger.info("Unsubscribed ${success}/${names.size()} tags for request ${request.requestName}.")
    }

    @Override
    // In cached connection mode close() should just release the leased connection.
    // Keep unsubscribe behavior explicit via unsubscribeAll().
    void close() { }

    void unsubscribeAll() {
        try {
            tool.unsubscribeAll(connection, 3000L)
            // tool.removeConnection(connectionString)
        } catch (Throwable ignored) { }
    }

    private EntityValue buildParameterLog(EntityValue parameter) {
        if (!parameter) return null
        return ec.entity.makeValue("moqui.math.ParameterLog")
            .setFields(parameter.getMap(), true, null, false).setSequencedIdPrimary()
    }
}
