/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.jms;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.amqp.AmqpTransportFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.ThrowingSupplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/**
 * A common test fixture to create a broker and connection factories for {@link JmsIOIT} & {@link
 * JmsIOTest}.
 */
public class CommonJms implements Serializable {
  private static final String BROKER_WITHOUT_PREFETCH_PARAM = "?jms.prefetchPolicy.all=0&";

  // convenient typedefs and a helper conversion functions
  private interface ThrowingSerializableSupplier<T> extends ThrowingSupplier<T>, Serializable {}

  private interface SerializableSupplier<T> extends Serializable, Supplier<T> {}

  private static <T> SerializableSupplier<T> toSerializableSupplier(
      ThrowingSerializableSupplier<T> throwingSerializableSupplier) {
    return () -> {
      try {
        return throwingSerializableSupplier.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  private static <T> SerializableFunction<Void, T> toSerializableFunction(Supplier<T> supplier) {
    return __ -> supplier.get();
  }

  static final String USERNAME = "test_user";
  static final String PASSWORD = "test_password";
  static final String QUEUE = "test_queue";
  static final String TOPIC = "test_topic";

  private final String brokerUrl;
  private final Integer brokerPort;
  private final String forceAsyncAcksParam;
  private final boolean useProviderFn;
  private transient BrokerService broker;

  private final Class<? extends ConnectionFactory> connectionFactoryClass;
  private final SerializableFunction<Void, ConnectionFactory> memoizedConnectionFactoryProviderFn;
  private final SerializableFunction<Void, ConnectionFactory>
      memoizedConnectionFactoryWithSyncAcksAndWithoutPrefetchProviderFn;

  public CommonJms(
      String brokerUrl,
      Integer brokerPort,
      String forceAsyncAcksParam,
      boolean useProviderFn,
      Class<? extends ConnectionFactory> connectionFactoryClass) {
    this.brokerUrl = brokerUrl;
    this.brokerPort = brokerPort;
    this.forceAsyncAcksParam = forceAsyncAcksParam;
    this.connectionFactoryClass = connectionFactoryClass;
    this.useProviderFn = useProviderFn;
    this.memoizedConnectionFactoryProviderFn =
        toSerializableFunction(
            Suppliers.memoize(toSerializableSupplier(this::createConnectionFactory)));
    this.memoizedConnectionFactoryWithSyncAcksAndWithoutPrefetchProviderFn =
        toSerializableFunction(
            Suppliers.memoize(
                toSerializableSupplier(
                    this::createConnectionFactoryWithSyncAcksAndWithoutPrefetch)));
  }

  void startBroker() throws Exception {
    broker = new BrokerService();
    broker.setUseJmx(false);
    broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
    TransportFactory.registerTransportFactory("amqp", new AmqpTransportFactory());
    if (connectionFactoryClass != ActiveMQConnectionFactory.class) {
      broker.addConnector(String.format("%s:%d?transport.transformer=jms", brokerUrl, brokerPort));
    } else {
      broker.addConnector(brokerUrl);
    }
    broker.setBrokerName("localhost");
    broker.setPopulateJMSXUserID(true);
    broker.setUseAuthenticatedPrincipalForJMSXUserID(true);
    broker.getManagementContext().setCreateConnector(false);

    // enable authentication
    List<AuthenticationUser> users = new ArrayList<>();
    // username and password to use to connect to the broker.
    // This user has users privilege (able to browse, consume, produce, list destinations)
    users.add(new AuthenticationUser(USERNAME, PASSWORD, "users"));
    SimpleAuthenticationPlugin plugin = new SimpleAuthenticationPlugin(users);
    BrokerPlugin[] plugins = new BrokerPlugin[] {plugin};
    broker.setPlugins(plugins);

    broker.start();
    broker.waitUntilStarted();
  }

  ConnectionFactory createConnectionFactory()
      throws NoSuchMethodException, InvocationTargetException, InstantiationException,
          IllegalAccessException {
    return connectionFactoryClass.getConstructor(String.class).newInstance(brokerUrl);
  }

  ConnectionFactory createConnectionFactoryWithSyncAcksAndWithoutPrefetch()
      throws NoSuchMethodException, InvocationTargetException, InstantiationException,
          IllegalAccessException {
    return connectionFactoryClass
        .getConstructor(String.class)
        .newInstance(brokerUrl + BROKER_WITHOUT_PREFETCH_PARAM + forceAsyncAcksParam);
  }

  void stopBroker() throws Exception {
    broker.stop();
    broker.waitUntilStopped();
    broker = null;
  }

  Class<? extends ConnectionFactory> getConnectionFactoryClass() {
    return this.connectionFactoryClass;
  }

  private SerializableFunction<Void, ConnectionFactory> selectDynamicOrMemoizedProviderFn(
      SerializableFunction<Void, ConnectionFactory> dynamicProviderFn,
      SerializableFunction<Void, ConnectionFactory> memoizedProviderFn) {
    if (useProviderFn) {
      // the provider function will instantiate a new CF each time it's invoked
      return dynamicProviderFn;
    } else {
      // or it will just return the memoized instance
      return memoizedProviderFn;
    }
  }

  public SerializableFunction<Void, ConnectionFactory> getConnectionFactoryProviderFn() {
    return selectDynamicOrMemoizedProviderFn(
        toSerializableFunction(toSerializableSupplier(this::createConnectionFactory)),
        memoizedConnectionFactoryProviderFn);
  }

  public SerializableFunction<Void, ConnectionFactory>
      getConnectionFactoryWithSyncAcksAndWithoutPrefetchProviderFn() {
    return selectDynamicOrMemoizedProviderFn(
        toSerializableFunction(
            toSerializableSupplier(this::createConnectionFactoryWithSyncAcksAndWithoutPrefetch)),
        memoizedConnectionFactoryWithSyncAcksAndWithoutPrefetchProviderFn);
  }

  public ConnectionFactory getConnectionFactory() {
    return memoizedConnectionFactoryProviderFn.apply(null);
  }

  public ConnectionFactory getConnectionFactoryWithSyncAcksAndWithoutPrefetch() {
    return memoizedConnectionFactoryWithSyncAcksAndWithoutPrefetchProviderFn.apply(null);
  }

  /** A test class that maps a {@link javax.jms.BytesMessage} into a {@link String}. */
  public static class BytesMessageToStringMessageMapper implements JmsIO.MessageMapper<String> {

    @Override
    public String mapMessage(Message message) throws Exception {
      BytesMessage bytesMessage = (BytesMessage) message;

      byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];

      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  public <T extends JmsIO.ConnectionFactoryContainer<T>> T withConnectionFactory(
      T connectionFactoryContainer,
      SerializableFunction<Void, ConnectionFactory> connectionFactoryProviderFn) {
    if (useProviderFn) {
      connectionFactoryContainer =
          connectionFactoryContainer.withConnectionFactoryProviderFn(connectionFactoryProviderFn);
    } else {
      // unwrap the memoized CF from the provider and pass it as-is to the connector
      connectionFactoryContainer =
          connectionFactoryContainer.withConnectionFactory(connectionFactoryProviderFn.apply(null));
    }

    return connectionFactoryContainer;
  }

  // Every test param list will be exploded into two with a different boolean flag attached, this
  // way we test two kinds of JmsIOs - one that uses ConnectionFactory directly and the other that
  // uses a provider function - depending on the value of that flag
  static Stream<List<Object>> crossProductWithBoolean(Stream<List<Object>> paramListStream) {
    return paramListStream.flatMap(
        paramList ->
            Stream.of(true, false)
                .map(
                    booleanFlag -> {
                      ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
                      builder.addAll(paramList);
                      builder.add(booleanFlag);
                      return builder.build();
                    }));
  }

  static Collection<Object[]> collectParams(Stream<List<Object>> paramListStream) {
    return paramListStream.map(l -> l.toArray(new Object[0])).collect(Collectors.toList());
  }
}
