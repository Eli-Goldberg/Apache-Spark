package dev.structured.streaming

import scala.io.Source
import java.io._
import java.net._

// Simple server which listens to a specific port.
// When a Spark Socket Stream is being conected, the server
// starts to read the file that was provided using the `fileName` arg
// and starts writing to the output stream each line to be consumed
// by the Spark application.

object Broadcast {
  def main(args: Array[String]) {
    args match {
      case Array(port: String, fileName: String) => {
        val serverSocket = new ServerSocket(port.toInt)
        val socket = serverSocket.accept()
        val out: PrintWriter = new PrintWriter(new OutputStreamWriter(socket.getOutputStream))

        for (line <- Source.fromFile(fileName).getLines) {
          println(line.toString)
          out.println(line.toString)
          out.flush()
          Thread.sleep(1000)
        }

        socket.close()
        sys.exit(0)
      }
      case _ => {
        System.err.println("Usage: `scala Broadcast.scala [port] [fileName]`")
        sys.exit(1)
      }
    }
  }
}