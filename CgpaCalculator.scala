import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import spark.implicits._

object CgpaCalculator {
  def main(args: Array[String]) {

    println("Grades Calculator in Scala using Spark!!!")
    println("----------------------------------")
    println("Enter No. of students:")
    val students = scala.io.StdIn.readInt()
    println("Enter No. of subjects:")
    val subjects = scala.io.StdIn.readInt()
    println("Enter No. of Partisions:")
    val partisions = scala.io.StdIn.readInt()

    //genrate random no 
    val start = 40
    val end   = 90
    val rnd = new scala.util.Random
    
    var marks = Array.ofDim[Int](students,subjects)
    for (i <- 0 until students) {
      for (j <- 0 until subjects) {
        var randomMakrs = start + rnd.nextInt( (end - start) + 1 )
         marks(i)(j) = randomMakrs
      }
    }

    val rdd1 = spark.sparkContext.parallelize(marks,partisions)
    val newT = new MyTokenlizer()
    var result = rdd1.map(newT.calculateGrades)
    

    if(result.count() > 0){
      var count : Int = 0
      val columnsNames = new Array[String]((subjects*2)+1)
      for (i <- 0 until subjects) {
        if(count == 0){
          columnsNames(count) = "subject " + (i+1)
          columnsNames(count+1) = "GPA " + (i+1)
          count += 2
        } else {
          columnsNames(count) = "subject " + (i+1)
          columnsNames(count+1) = "GPA " + (i+1)
          count += 2
        }
      }
      val resiltList = result.collect.toList
      columnsNames(subjects*2) = "CGPA"
      for (i <- 0 until resiltList.length) {
        println(s"Roll No: "+(i+1))
        val cgpa = resiltList(i)(subjects*2)
        println(s"CGAP : $cgpa")
      }
       //show data in table
      val dataFrame =result.toDF("Results")
      dataFrame.show()
      // dataFrame.withColumn("_tmp", dataFrame("test1")).select(
      //   $"_tmp".getItem(0).as("col1"),
      //   $"_tmp".getItem(1).as("col2"),
      //   $"_tmp".getItem(2).as("col3")
      // ).drop("_tmp").show()
      
    }
   
  }
}	
class MyTokenlizer extends Serializable {
  
  def calculateGrades(rdd: Array[Int]): Array[Int] = {

    var grade : Double = 0
    var total : Double = 0
    var sum : Int = 0
    var totalGrades = new Array[Int]((rdd.length * 2)+1)
    var count : Int = 0
    for (i <- 0 until rdd.length) {
      var marks = rdd(i)
      val grade: Int = marks match {
        case marks if marks < 50  => 0
        case marks if marks >= 50 && marks < 58   =>  1
        case marks if marks >= 58 && marks < 70   => 2
        case marks if marks >= 70 && marks < 85   => 3
        case marks if marks >= 85 => 4
        case _ => 1
      }
      total = total + (grade * 3)
      sum += 3
      if(count == 0){
        totalGrades(count) = marks
        totalGrades(count+1) = grade
        count += 2
      } else {
        totalGrades(count) = marks
        totalGrades(count+1) = grade
        count += 2
      }
    }
    
    val cgpa = total/sum
    totalGrades(rdd.length * 2) = cgpa.toInt
    // println(s"Cgpa is $cgpa")
    
    return totalGrades
  }
} 