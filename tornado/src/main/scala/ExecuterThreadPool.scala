import java.net.{Socket, ServerSocket}
import java.util.concurrent.{Executors, ExecutorService}
import java.util.Date

class NetworkService(port: Int, poolSize: Int) extends Runnable {
  def run() {
    while (true) {
      // This will block until a connection comes in.
      println("--------------",Thread.currentThread().getName)
//      val socket = serverSocket.accept()
//      (new Handler(socket)).run()
    }
  }
}

class Handler(socket: Socket) extends Runnable {
  def message = (Thread.currentThread.getName() + "\n").getBytes

  def run() {
    socket.getOutputStream.write(message)
    socket.getOutputStream.close()
  }
}
object Mains extends App{
  (new Thread((new NetworkService(2020, 2)),"thread-uni-1")).start()
  (new Thread((new NetworkService(200, 2)),"thread-uni-2")).start()
}
