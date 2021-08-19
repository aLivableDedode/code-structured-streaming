package structuredops

object KafakSourceDemo {
  def main(args: Array[String]): Unit = {
    val name = System.getProperty("os.name").toUpperCase()
    val arch = System.getProperty("os.arch").toUpperCase()
    val version = System.getProperty("os.version").toUpperCase()
    println(s"$name + $arch + $version" )
  }

}
