package okapies.finagle.kafka.transport

import java.net.{InetSocketAddress, SocketAddress}
import java.security.cert.Certificate

import com.twitter.finagle.Status
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise, Time}
import kafka.producer.Producer
import kafka.producer.KeyedMessage

class ProducerTransport[K,V](producer: Producer[K,V])
 extends Transport[KeyedMessage[K,V], Unit] {

  override def remoteAddress: SocketAddress = new InetSocketAddress(0)

  override def peerCertificate: Option[Certificate] = None

  override def localAddress: SocketAddress = new InetSocketAddress(0)

  override def status: Status = Status.Open

  override val onClose: Future[Throwable] = Promise()

  override def close(deadline: Time): Future[Unit] = Future(producer.close)

  override def write(req: KeyedMessage[K,V]): Future[Unit] = Future(producer.send(req))

  override def read(): Future[Unit] = Future(())
}
