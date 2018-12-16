package com.learning.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
case class TempData(day: Int, doy: Int, month: Int, year: Int,
    precip: Double, snow: Double, tave:Double, tmax: Double, tmin: Double)


object TempData{
  def toDoubleOrNeg(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case _:NumberFormatException => -1
    }
  }
  
  def main(args:Array[String]):Unit={
     /* val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()
    
    */
    
  val source =scala.io.Source.fromFile("C:\\DATA\\swapnil\\codebase\\sampledata\\tempdata.txt")
  val lines=source.getLines().drop(1)
  val data=lines.flatMap{ line =>
    val p=line.split(",")
      if(p(7)=="." || p(8)=="." || p(9)==".") Seq.empty else
    Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
          toDoubleOrNeg(p(5)), toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble, 
          p(9).toDouble))
    }.toArray
  
   data.take(5) foreach println
   //WHAT IS THE DATE OF THE HIGHEST TEMPERATURE
   val maxTemp=data.map(_.tmax).max
   val hotdays=data.filter(_.tmax == maxTemp)
   println("max temperature days are:"+hotdays.mkString)
  //getting highest temperature using efficent approches
   val hotdays2=data.reduceLeft((a,b) => if(a.tmax > b.tmax) a else b)
  
   //find the days that have 1 or more than 1 precentiattion
   val rainycount=data.count(_.precip >1)
   
  //average high temperatire for rainy days
   val (rainysum,rainycount2)=data.foldLeft(0.0->0)((sumCounttuple,td)=> (if(td.precip >1) (sumCounttuple._1 +td.tmax,sumCounttuple._2+1) else (sumCounttuple._1,sumCounttuple._2))) 
       //using pattern matching for the above case
  
   val (rainysum1,rainycount3)=data.foldLeft(0.0->0){ case((sum,count),td) =>
      if(td.precip >1) (sum+td.tmax,count +1) else (sum,count)
    }
    
    
    //use of flatmap for the same example
    
    val rainycount5=data.flatMap(td => if(td.precip >1) Seq.empty else Seq(td.tmax))
   println(s"Averge rainy temperature ${rainysum/rainycount2}")
   println(s"Averge rainy temperature using pattern match ${rainysum1/rainycount3}")
   println(s"Averge rainy temperature using flat map  ${rainycount5.sum/rainycount5.length}")
   
   //find the average temperature by month
   
   val monthgroup=data.groupBy(_.month)
   
   //using normal method
   val monthlyTemp=monthgroup.map{x => 
     (x._1) ->x._2.foldLeft(0.0)((sum,td)=> (sum+td.tmax)/x._2.length)
    }
   //using pattern matching as its good to use that when procesing on a tuple
    val monthlyTemp1=monthgroup.map{
      case(month,tmpdata) =>
        month ->tmpdata.foldLeft(0.0)((sum,td) => (sum+td.tmax)/tmpdata.length)
    }
   
    println("monthly average temperature:"+monthlyTemp)
    println("monthly average temperature using group by :"+monthlyTemp)
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
  }
}
