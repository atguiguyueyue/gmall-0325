import com.alibaba.fastjson.JSON

object test {
  def main(args: Array[String]): Unit = {

    val zs: Student = new Student("1","zs")
//    JSON.toJSONString(zs)

  }
}

case class Student(
                    id: String,
                    name: String)