package okapies.finagle.kafka.transport

import java.net.{InetSocketAddress, SocketAddress}
import java.util.Properties
import java.util.logging.Logger

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.{ListeningServer, _}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.client.{StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.{SerialClientDispatcher, SerialServerDispatcher}
import com.twitter.finagle.server.{Listener, StackServer, StdStackServer}
import com.twitter.finagle.service._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.util.{Timer, Decoder => _, Encoder => _, _}
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector}
import kafka.message.MessageAndMetadata
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer._

case class Config(prop:Properties) {
  def mk(): (Config, Stack.Param[Config]) =
    (this, Config.param)
}

object Config {
  implicit val param = Stack.Param(Config(new Properties()))
}

case class Topic(topic:Option[String]) {
  def mk(): (Topic, Stack.Param[Topic]) =
    (this, Topic.param)
}

object Topic {
  implicit val param = Stack.Param(Topic(None))
}

case class KeyDecoder(decoder:Decoder[_]) {
  def mk(): (KeyDecoder, Stack.Param[KeyDecoder]) =
    (this, KeyDecoder.param)
}

object KeyDecoder {
  implicit val param = Stack.Param(KeyDecoder(new DefaultDecoder))
}

case class ValueDecoder(decoder:Decoder[_]) {
  def mk(): (ValueDecoder, Stack.Param[ValueDecoder]) =
    (this, ValueDecoder.param)
}

object ValueDecoder {
  implicit val param = Stack.Param(ValueDecoder(new DefaultDecoder))
}

case class KafkaConsumer[K,V](stack: Stack[ServiceFactory[MessageAndMetadata[K,V], Any]]
                              = StackServer.newStack[MessageAndMetadata[K,V], Any]
                              ,
                              params: Params =
                              StackServer.defaultParams
                                + Config.param.default
                                + KeyDecoder.param.default
                                + ValueDecoder.param.default
                                + Topic.param.default)
  extends StdStackServer[MessageAndMetadata[K,V],Any,KafkaConsumer[K,V]] {
  override type In = Any
  override type Out = MessageAndMetadata[K,V]

  override protected def newListener(): Listener[In, Out] = new Listener[In,Out] {
    override def listen(addr: SocketAddress)(serveTransport: (Transport[In, Out]) => Unit): ListeningServer = {
      val Config(props) = params[Config]
      val KeyDecoder(keyDecoder:Decoder[K]) = params[KeyDecoder]
      val ValueDecoder(valueDecoder:Decoder[V]) = params[ValueDecoder]
      val Topic(topic) = params[Topic]
      topic match {
        case Some(topicName) =>
          props.setProperty("zookeeper.connect", addr match {
            case a:InetSocketAddress => s"${a.getHostName}:${a.getPort}"
          })
          val consumer: ConsumerConnector = Consumer.create(new ConsumerConfig(props))
          val transport: Transport[In, Out] = new ConsumerTransport(consumer, topicName, keyDecoder, valueDecoder)
          serveTransport(transport)

          val server = new ListeningServer with CloseAwaitably {
            var _status = Status.Open
            def status:Status = _status

            def closeServer(deadline: Time) = { println("Closing!!!"); closeAwaitably { Future.Done }}
            val boundAddress = new InetSocketAddress(0)
          }
//          transport.onClose.ensure(server.close())
          server
        case None => throw new IllegalArgumentException("Topic config is required")
      }

    }
  }

  override protected def newDispatcher(transport: Transport[In, Out], service: Service[MessageAndMetadata[K,V], Any]): Closable = {
    val timer = params[param.Timer].timer
    new SerialServerDispatcher(new TransportProxy(transport) {
      override def write(req: Any): Future[Unit] = transport.write(req)

      override def read(): Future[MessageAndMetadata[K, V]] = service.status match {
        case Status.Open => transport.read()
        case _ =>
          val p = Promise[MessageAndMetadata[K, V]]()
          timer.doLater(Duration.fromSeconds(1))(p.become(read()))
          p
      }
    }, service)
  }


  override def serve(addr: SocketAddress, factory: ServiceFactory[MessageAndMetadata[K, V], Any]): ListeningServer = {
    val timer = params[param.Timer].timer
    val statsReceiver = params[param.Stats].statsReceiver
    val label = params[param.Label].label
    val logger = params[param.Logger].log
    val classifier = params[param.ResponseClassifier].responseClassifier

    super.serve(addr,
      new FailureAccrualFactory(
        factory,
        1,
        Duration.fromSeconds(10),
        timer,
        statsReceiver,
        label,
        logger,
        Address(addr.asInstanceOf[InetSocketAddress]),
        classifier
      ))
  }

  override protected def copy1(stack: Stack[ServiceFactory[MessageAndMetadata[K, V], Any]] = this.stack, params: Params = this.params): KafkaConsumer[K,V] = copy(stack, params)

  def withProperties(props: scala.collection.Map[String, String]): KafkaConsumer[K,V] = {
    val Config(properties) = params[Config]
    props.foreach { case (k,v) =>  properties.setProperty(k,v)}
    this
  }

  def withKeyDecoder[NK](decoder:Decoder[NK]):KafkaConsumer[NK,V] = {
    configured(KeyDecoder(decoder)).asInstanceOf[KafkaConsumer[NK,V]]
  }

  def withValueDecoder[NV](decoder:Decoder[NV]):KafkaConsumer[K,NV] = {
    configured(ValueDecoder(decoder)).asInstanceOf[KafkaConsumer[K,NV]]
  }

  def withGroup(group: String, topic: String): KafkaConsumer[K,V] = {
    params[Config].prop.setProperty("group.id", group)
    configured(Topic(Some(topic)))
  }
}

case class KafkaProducer[K,V](stack: Stack[ServiceFactory[KeyedMessage[K,V], Unit]] = StackClient.newStack[KeyedMessage[K,V], Unit],
                              params: Params = StackClient.defaultParams + Config(new Properties))
  extends StdStackClient[KeyedMessage[K,V], Unit, KafkaProducer[K,V]] {
  override type In = KeyedMessage[K,V]
  override type Out = Unit

  override protected def copy1(stack: Stack[ServiceFactory[KeyedMessage[K,V], Unit]] = this.stack,
                               params: Params = this.params): KafkaProducer[K,V] =
    copy(stack, params)

  override protected def newDispatcher(transport: Transport[In, Out]): Service[KeyedMessage[K,V], Unit] = {
    val param.Stats(receiver) = params[param.Stats]
    new SerialClientDispatcher(transport, receiver)
  }

  override protected def newTransporter(): Transporter[In, Out] = new Transporter[In,Out] {
    override def apply(addr: SocketAddress): Future[Transport[KeyedMessage[K,V], Unit]] = {
      val Config(props) = params[Config]
      addr match {
        case a:InetSocketAddress =>
          props.setProperty("metadata.broker.list", s"${a.getHostName}:${a.getPort}")
      }
      Future {
        val producer:Producer[K,V] = new Producer(new ProducerConfig(props))
        new ProducerTransport(producer)
      }
    }
  }

  def withProperties(props: scala.collection.Map[String, String]): KafkaProducer[K,V] = {
    val Config(properties) = params[Config]
    props.foreach { case (k,v) =>  properties.setProperty(k,v)}
    this
  }

  def withKeyEncoder[NK](encoder:Class[_ <: Encoder[NK]]): KafkaProducer[NK,V] = {
    withProperties(Map("key.serializer.class" -> encoder.getCanonicalName)).asInstanceOf[KafkaProducer[NK,V]]
  }

  def withValueEncoder[NV](encoder:Class[_ <: Encoder[NV]]): KafkaProducer[K,NV] = {
    withProperties(Map("serializer.class" -> encoder.getCanonicalName)).asInstanceOf[KafkaProducer[K,NV]]
  }
}

object KafkaClient {
  def consumer = KafkaConsumer()

  def producer = KafkaProducer()
}