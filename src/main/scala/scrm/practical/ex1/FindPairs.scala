package scrm.practical.ex1

import scala.io.Source
import scala.jdk.CollectionConverters._

object FindPairs {


  def findPairs(elements: Seq[Int], targetValue: Int): Seq[(Int, Int)] = {
    val sv1: LazyList[Int] = elements.to(LazyList)
    val sv2: LazyList[Int] = elements.to(LazyList)
    findPairs(sv1, sv2, targetValue)
  }

  def findPairsOnLargeFile(file: String, targetValue: Int): Seq[(Int, Int)] = {
    val sv1 = Source.fromResource(file).getLines().to(LazyList)
    val sv2 = Source.fromResource(file).getLines().to(LazyList)
    findPairs(sv1, sv2, targetValue)
  }

  private def findPairs(s1: LazyList[Any], s2: LazyList[Any], targetValue: Int): Seq[(Int, Int)] = {
    def runPrecondition(v1: Any, v2: Any) = {
      (v1, v2) match {
        case (v1: Int, v2: Int) => (v1 + v2 == targetValue, Some(v1, v2))
        case (v1: String, v2: String) => (v1.toInt + v2.toInt == targetValue, Some(v1.toInt, v2.toInt))
        case (_, _) => (false, None)
      }
    }

    def buildPairs(): LazyList[(Int, Int)] = {
      for (
        sv1 <- s1;
        sv2 <- s2;
        p = runPrecondition(sv1,sv2);
        v = p._2 if p._1
      ) yield v.get
    }

    buildPairs()
      .map { case (a, b) => if (a > b) (a, b) else (b, a) }
      .distinct
  }


}
