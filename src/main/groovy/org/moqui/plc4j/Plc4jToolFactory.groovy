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

import groovy.transform.CompileStatic
import java.util.concurrent.TimeUnit
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import org.moqui.BaseException
import org.moqui.context.ExecutionContextFactory
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.context.ToolFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.plc4x.java.api.PlcDriverManager
import org.apache.plc4x.java.api.PlcConnection
import org.apache.plc4x.java.api.PlcConnectionManager
import org.apache.plc4x.java.utils.cache.CachedPlcConnectionManager
import org.apache.plc4x.java.api.authentication.PlcAuthentication
import org.apache.plc4x.java.api.authentication.PlcUsernamePasswordAuthentication
import org.apache.plc4x.java.api.exceptions.PlcConnectionException
import org.apache.plc4x.java.api.exceptions.PlcTimeoutException
import org.apache.plc4x.java.api.model.PlcSubscriptionHandle
import org.apache.plc4x.java.api.messages.PlcUnsubscriptionRequest
import org.apache.plc4x.java.api.messages.PlcUnsubscriptionResponse

/** A ToolFactory for Apache Plc4j, a set of libraries for communicating with industrial programmable logic controllers (PLCs),
    drives, remote IO, etc... using a variety of protocols but with a shared API. */
@CompileStatic
class Plc4jToolFactory implements ToolFactory<Plc4jToolFactory> {
    protected final static Logger logger = LoggerFactory.getLogger(Plc4jToolFactory.class)
    final static String TOOL_NAME = "Plc4j"

    protected ExecutionContextFactoryImpl ecfi = null
    protected boolean cachedConnection = true
    protected PlcConnectionManager connectionManager = null
    // only used in non-cached mode
    protected final ConcurrentMap<String, PlcConnection> connectionRegistry = new ConcurrentHashMap<>()
    /** Thread-safe map of subscription name -> handle. */
    protected final ConcurrentMap<String, PlcSubscriptionHandle> subscriptionRegistry = new ConcurrentHashMap<>()
    // Guard to block new clients while tearing down
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false)

    /** Default empty constructor */
    Plc4jToolFactory() { }

    @Override
    String getName() { return TOOL_NAME }

    @Override
    void init(ExecutionContextFactory ecf) {
        logger.info("Initializing Plc4j.")
        this.ecfi = (ExecutionContextFactoryImpl) ecf
        this.cachedConnection = Boolean.getBoolean("plc4j_cached_connection")
        if (cachedConnection) {
            connectionManager = CachedPlcConnectionManager.getBuilder().build()
            logger.info("Plc4jToolFactory running in cached mode.")
        } else {
            connectionManager = PlcDriverManager.getDefault().getConnectionManager()
            logger.info("Plc4jToolFactory running in non-cached mode.")
        }
    }

    @Override
    void preFacadeInit(ExecutionContextFactory ecf) { }

    @Override
    Plc4jToolFactory getInstance(Object... parameters) {
        if (connectionManager == null) throw new IllegalStateException("Plc4jToolFactory not initialized")
        return this
    }

    @Override
    void destroy() {
        removeConnections()
    }

    ExecutionContextFactory getEcf() { return ecfi }

    // Connections

    /**
     * Connects to a PLC using the given connection string.
     *
     * @param connectionString: PLC connection string.
     * @return PlcConnection instance.
     * @throws BaseException if the connection attempt failed.
     */
    PlcConnection getConnection(String connectionString) {
        if (shuttingDown.get()) throw new BaseException("Plc4jToolFactory is shutting down; refusing new connections.")
        if (!connectionString) throw new IllegalArgumentException("connectionString must not be empty")

        // In cached mode, defer to the cached connection manager/pool
        if (cachedConnection) {
            try {
                return connectionManager.getConnection(connectionString)
            } catch (PlcConnectionException | PlcTimeoutException e) {
                throw new BaseException("Failed to connect to ${connectionString}: ${e.class.simpleName}: ${e.message}", e)
            }
        }

        // Non-cached mode, maintain exactly one connection per connectionString
        try {
            return connectionRegistry.compute(connectionString) { k, existingConnection ->
                try {
                    if (existingConnection?.isConnected()) return existingConnection
                    // Clean up stale/disconnected instance before replacing
                    if (existingConnection != null) {
                        try { existingConnection.close() } catch (Throwable ignore) { }
                    }
                    logger.info("Registering new connection for connection string ${connectionString}.")
                    PlcConnection newConnection = connectionManager.getConnection(connectionString)
                    return newConnection
                } catch (PlcConnectionException | PlcTimeoutException e) {
                    throw e
                }
            }
        } catch (PlcConnectionException | PlcTimeoutException e) {
            connectionRegistry.remove(connectionString)
            throw new BaseException("Failed to connect to ${connectionString}: ${e.class.simpleName}: ${e.message}", e)
        }
    }

    /**
    * Connects to a PLC using the given connection string and authentication credentials.
    *
    * @param connectionString PLC connection string.
    * @param username non-empty username
    * @param password non-empty password
    * @return PlcConnection instance.
    * @throws BaseException if the connection attempt failed.
    */
    PlcConnection getConnection(String connectionString, String username, String password) {
        if (shuttingDown.get()) throw new BaseException("Plc4jToolFactory is shutting down; refusing new connections.")
        if (!connectionString) throw new IllegalArgumentException("connectionString must not be empty")
        if (!username?.trim()) throw new IllegalArgumentException("Username must not be blank")
        if (!password?.trim()) throw new IllegalArgumentException("Password must not be blank")

        final PlcAuthentication auth = new PlcUsernamePasswordAuthentication(username, password)

        // In cached mode, defer to cached connection manager/pool
        if (cachedConnection) {
            try {
                logger.info("Requesting cached authenticated connection for ${connectionString} (user=${username}).")
                return connectionManager.getConnection(connectionString, auth)
            } catch (PlcConnectionException | PlcTimeoutException e) {
                throw new BaseException("Failed to connect to ${connectionString} as ${username}: ${e.class.simpleName}: ${e.message}", e)
            }
        }

        // Non-cached mode: maintain exactly one connection per connectionString.
        try {
            return connectionRegistry.compute(connectionString) { k, existingConnection ->
                try {
                    if (existingConnection?.isConnected()) return existingConnection
                    if (existingConnection != null) {
                        try { existingConnection.close() } catch (Throwable ignore) { }
                    }
                    logger.info("Registering new authenticated connection for ${connectionString} (user=${username}).")
                    PlcConnection newConnection = connectionManager.getConnection(connectionString, auth)
                    return newConnection
                } catch (PlcConnectionException | PlcTimeoutException e) {
                    throw e
                }
            }
        } catch (PlcConnectionException | PlcTimeoutException e) {
            connectionRegistry.remove(connectionString)
            throw new BaseException("Failed to connect to ${connectionString} as ${username}: ${e.class.simpleName}: ${e.message}", e)
        }
    }

    /** Safely close a PlcConnection, swallowing any errors. */
    private static void safeClose(PlcConnection connection) {
        if (connection == null) return
        try {
            if (connection.isConnected()) connection.close()
        } catch (Throwable ignore) { /* noop */ }
    }

    /**
    * Remove a PLC connection from (non-cached) connection map and close it.
    *
    * @param connectionString plc connection string
    */
    void removeConnection(String connectionString) {
        if (!connectionString) {
            logger.warn("removeConnection called with empty connectionString.")
            return
        }

        PlcConnection removedConnection = connectionRegistry.remove(connectionString)
        if (removedConnection != null) {
            safeClose(removedConnection)
            logger.info("PLC connection removed for ${connectionString}.")
        } else {
            if (cachedConnection) {
                logger.info("No tracked connection for ${connectionString} to remove (cached connection mode).")
            } else {
                logger.warn("The connectionString ${connectionString} is not registered; nothing to remove.")
            }
        }
    }

    /** Remove all PLC connections and clear subscriptions. */
    void removeConnections() {
        shuttingDown.set(true)
        try {
            List<PlcConnection> snapshot = new ArrayList<>(connectionRegistry.values())
            connectionRegistry.clear()
            snapshot.each { connection -> safeClose(connection) }
            subscriptionRegistry.clear()
            logger.info("All PLC connections removed${cachedConnection ? ' (cached connection mode active)' : ''}.")
        } finally {
            shuttingDown.set(false)
        }
    }

    /** Return the existing registered connection (may be null). */
    PlcConnection getExistingConnection(String connectionString) {
        return connectionString ? connectionRegistry.get(connectionString) : null
    }

    /** True if we have a registered connection and it's connected. */
    boolean isConnected(String connectionString) {
        PlcConnection connection = getExistingConnection(connectionString)
        return connection != null && connection.isConnected()
    }

    /** Current number of registered connections. */
    int connectionCount() { return connectionRegistry.size() }

    /** Snapshot of registered connection strings. */
    List<String> listConnectionStrings() { return new ArrayList<>(connectionRegistry.keySet()) }

    /**
    * Ensure there is a connected instance in the registry.
    * Reuses if connected, otherwise (re)opens and stores it.
    */
    PlcConnection ensureConnected(String connectionString) {
        if (!connectionString) throw new IllegalArgumentException("connectionString must not be empty")
        if (shuttingDown.get()) throw new BaseException("Plc4jToolFactory is shutting down; refusing new connections.")

        return connectionRegistry.compute(connectionString) { k, existing ->
            if (existing != null && existing.isConnected()) return existing
            logger.info("Opening (or reopening) connection for ${connectionString}")
            try {
                PlcConnection c = connectionManager.getConnection(connectionString)
                return c
            } catch (PlcConnectionException | PlcTimeoutException e) {
                logger.error("Failed to (re)connect ${connectionString}", e)
                return null
            }
        }
    }

    /**
    * Ensure there is a connected instance in the registry with the provided credentials.
    * Reuses if connected, otherwise (re)opens and stores it.
    */
    PlcConnection ensureConnected(String connectionString, String username, String password) {
        if (!connectionString) throw new IllegalArgumentException("connectionString must not be empty")
        if (!username) throw new IllegalArgumentException("username must not be null or empty")
        if (!password) throw new IllegalArgumentException("password must not be null or empty")
        if (shuttingDown.get()) throw new BaseException("Plc4jToolFactory is shutting down; refusing new connections.")

        return connectionRegistry.compute(connectionString) { k, existing ->
            if (existing != null && existing.isConnected()) return existing
            logger.info("Opening (or reopening) connection for ${connectionString} with credentials")
            try {
                PlcAuthentication auth = new PlcUsernamePasswordAuthentication(username, password)
                PlcConnection c = connectionManager.getConnection(connectionString, auth)
                return c
            } catch (PlcConnectionException | PlcTimeoutException e) {
                logger.error("Failed to (re)connect ${connectionString} with credentials", e)
                return null
            }
        }
    }

    /**
    * Force a reconnect: close and remove any existing, then open fresh and register.
    * Useful after connection errors or protocol timeouts.
    */
    PlcConnection reconnect(String connectionString) {
        if (!connectionString) throw new IllegalArgumentException("connectionString must not be empty")
        removeConnection(connectionString)   // your improved version that closes if needed
        return ensureConnected(connectionString)
    }

    PlcConnection reconnect(String connectionString, String username, String password) {
        if (!connectionString) throw new IllegalArgumentException("connectionString must not be empty")
        removeConnection(connectionString)
        return ensureConnected(connectionString, username, password)
    }

    /**
    * Borrow a new ephemeral connection directly from the manager, run task, always close.
    * This never touches the registry and is ideal for short, isolated ops.
    */
    def <T> T withEphemeralConnection(String connectionString, Closure<T> task) {
        if (!connectionString) throw new IllegalArgumentException("connectionString must not be empty")
        if (task == null) throw new IllegalArgumentException("Task must not be null")
        PlcConnection connection = null
        try {
            connection = connectionManager.getConnection(connectionString)
            return task.call(connection)
        } finally {
            try { connection?.close() } catch (Throwable ignored) { }
        }
    }

    /**
    * Borrow a new ephemeral connection, with the specified credentials, directly from the manager, run task, always close.
    * This never touches the registry and is ideal for short, isolated ops.
    */
    def <T> T withEphemeralConnection(String connectionString, String username, String password, Closure<T> task) {
        if (!connectionString) throw new IllegalArgumentException("connectionString must not be empty")
        if (!username) throw new IllegalArgumentException("username must not be null or empty")
        if (!password) throw new IllegalArgumentException("password must not be null or empty")
        if (task == null) throw new IllegalArgumentException("task must not be null")
        PlcConnection connection = null
        try {
            PlcAuthentication auth = new PlcUsernamePasswordAuthentication(username, password)
            connection = connectionManager.getConnection(connectionString, auth)
            return task.call(connection)
        } finally {
            try { connection?.close() } catch (Throwable ignored) { }
        }
    }

    // Subscriptions

    /** 
     * Get the subscription handle for the given name (or null if missing).
     *
     * @param subscriptionName Unique subscription or tag name.
     * @return PlcSubscriptionHandle - PlcSubscriptionHandle associated with the specified subscription or tag name.
     */
    PlcSubscriptionHandle getSubscription(String subscriptionName) {
        if (!subscriptionName) return null
        return subscriptionRegistry.get(subscriptionName)
    }

    /** 
     * True if a subscription with this name is registered. 
     *
     * @param subscriptionName Unique subscription or tag name.
     * @return true if a name is registered
    */
    boolean hasSubscription(String subscriptionName) {
        return subscriptionName && subscriptionRegistry.containsKey(subscriptionName)
    }

    /**
    * Associate a subscription handle with a name.
    * If a previous handle exists it is returned; caller may choose to unsubscribe it.
    *
    * @param subscriptionName Unique subscription or tag name with which the PlcSubscriptionHandle is to be associated.
    * @param PlcSubscriptionHandle associated with the subscription or tag name.
    */
    PlcSubscriptionHandle putSubscription(String subscriptionName, PlcSubscriptionHandle subscriptionHandle) {
        if (!subscriptionName) throw new IllegalArgumentException("subscriptionName must not be null or empty.")
        if (subscriptionHandle == null) throw new IllegalArgumentException("subscriptionHandle must not be null.")
        return subscriptionRegistry.put(subscriptionName, subscriptionHandle)
    }

    /** Put multiple subscriptions at once; returns subscriptionNames that replaced existing entries. */
    Set<String> putSubscriptions(Map<String, PlcSubscriptionHandle> entries) {
        if (!entries) return Collections.emptySet()
        Set<String> replaced = new HashSet<>()
        entries.each { subscriptionName, handle ->
            PlcSubscriptionHandle prev = putSubscription(subscriptionName, handle)
            if (prev != null) replaced.add(subscriptionName)
        }
        return replaced
    }

    /** 
     * Remove a subscription entry by name.
     *
     * @param subscriptionName Unique subscription or tag subscriptionName for which the subscription handle is to be removed.
     */
    boolean removeSubscription(String subscriptionName) {
        if (!subscriptionName) throw new IllegalArgumentException("subscriptionName must not be null or empty.")
        return subscriptionRegistry.remove(subscriptionName)
    }

    /** Remove many subscription entries by name. */
    int removeSubscriptions(Collection<String> subscriptionNames) {
        if (!subscriptionNames) return 0
        int removed = 0
        subscriptionNames.each { subscriptionName -> if (subscriptionRegistry.remove(subscriptionName)) removed++ }
        return removed
    }

    /** Remove subscriptions whose names start with a prefix. */
    int removeSubscriptionsByPrefix(String prefix) {
        if (!prefix) return 0
        List<String> toRemove = new ArrayList<>(subscriptionRegistry.keySet().findAll { it.startsWith(prefix) })
        return removeSubscriptions(toRemove)
    }

    /** Snapshot of current subscription names. */
    List<String> listSubscriptionNames() {
        return new ArrayList<>(subscriptionRegistry.keySet())
    }

    /** Current subscription count. */
    int subscriptionCount() {
        return subscriptionRegistry.size()
    }

    // Unsubscribe

    /**
    * Unsubscribe a single registered subscription by name on the given connection,
    * then remove it from the local registry. Returns true if something was unsubscribed.
    * @param subscriptionName Unique subscription or tag name.
    * @param connection PlcConnection.
    * @param connection timeout
    */
    boolean unsubscribe(String subscriptionName, PlcConnection connection, long connectionTimeout = 3000L) {
        if (!subscriptionName) return false
        PlcSubscriptionHandle handle = subscriptionRegistry.get(subscriptionName)
        if (handle == null || connection == null || !connection.getMetadata().isSubscribeSupported()) return false

        try {
            final PlcUnsubscriptionRequest.Builder builder = connection.unsubscriptionRequestBuilder()
            builder.addHandles(handle)
            final PlcUnsubscriptionRequest request = builder.build()
            final PlcUnsubscriptionResponse response = request.execute().get(connectionTimeout, TimeUnit.MILLISECONDS)

            subscriptionRegistry.remove(subscriptionName)
            return true
        } catch (Throwable t) {
            logger.warn("Unsubscribe failed for ${subscriptionName}: ${t.class.simpleName}: ${t.message}")
            return false
        }
    }

    /**
    * Unsubscribe many by name (best-effort), removing successful ones from the registry.
    * Returns the number of names successfully unsubscribed.
    */
    int unsubscribe(Collection<String> subscriptionNames, PlcConnection connection, long connectionTimeout = 3000L) {
        if (!subscriptionNames) return 0
        List<PlcSubscriptionHandle> handles = subscriptionNames.collect { subscriptionRegistry.get(it) }.findAll { it != null }
        if (handles.isEmpty() || connection == null || !connection.getMetadata().isSubscribeSupported()) return 0

        int success = 0
        try {
            final PlcUnsubscriptionRequest.Builder builder = connection.unsubscriptionRequestBuilder()
            handles.each { handle -> builder.addHandles(handle) }
            final PlcUnsubscriptionRequest request = builder.build()
            final PlcUnsubscriptionResponse response = request.execute().get(connectionTimeout, TimeUnit.MILLISECONDS)

            subscriptionNames.each { subscriptionRegistry.remove(it) }
            success = handles.size()
        } catch (Throwable t) {
            logger.warn("Bulk unsubscribe failed (${handles.size()} handles): ${t.class.simpleName}: ${t.message}")
            subscriptionNames.each { subscriptionName -> if (unsubscribe(subscriptionName, connection, connectionTimeout)) success++ }
        }
        return success
    }

    /**
    * Unsubscribe everything currently tracked on this connection (best-effort).
    * Returns the number of subscriptions successfully unsubscribed.
    */
    int unsubscribeAll(PlcConnection connection, long connectionTimeout = 3000L) {
        List<String> subscriptionNames = listSubscriptionNames()
        return unsubscribe(subscriptionNames, connection, connectionTimeout)
    }

    /**
    * Unsubscribe all whose names start with the given prefix.
    * Returns the number successfully unsubscribed.
    */
    int unsubscribeByPrefix(String prefix, PlcConnection connection, long connectionTimeout = 3000L) {
        if (!prefix) return 0
        List<String> subscriptionNames = new ArrayList<>(subscriptionRegistry.keySet().findAll { it.startsWith(prefix) })
        return unsubscribe(subscriptionNames, connection, connectionTimeout)
    }

    // Clients

    /**
    * Get or create a FieldbusClient for the given connection string.
    *
    * @param connectionString plc connection string (required)
    * @return FieldbusClient
    * @throws BaseException if the factory is shutting down or the connection can’t be obtained
    */
    FieldbusClient getClient(String connectionString) {
        if (shuttingDown.get()) throw new BaseException("Plc4jToolFactory is shutting down; refusing new clients.")
        return new FieldbusClient(this.ecfi.eci, connectionString)
    }

    /**
    * Get or create a FieldbusClient for the given connection string using credentials.
    *
    * @param connectionString plc connection string (required)
    * @param username username (required)
    * @param password password (required)
    * @return FieldbusClient
    * @throws BaseException if the factory is shutting down or the connection can’t be obtained
    */
    FieldbusClient getClient(String connectionString, String username, String password) {
        if (shuttingDown.get()) throw new BaseException("Plc4jToolFactory is shutting down; refusing new clients.")
        return new FieldbusClient(this.ecfi.eci, connectionString, username, password)
    }
}
