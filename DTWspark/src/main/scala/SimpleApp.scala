import java.net.{ServerSocket, Socket}
import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source
import scala.Array.canBuildFrom
import scala.util.Sorting
import scala.math.Ordered
import org.apache.spark.rdd.CoalescedRDD
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.serializer._
import com.esotericsoftware.kryo.Kryo

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(SimpleApp.getClass)
  }
}


/// Data structure for sorting the query
class Index(val q:Double,val ind:Int)
{
 var value:Double=q
 var index: Int=ind 
}


/// Sorting function for the query, sort by abs(z_norm(q[i])) from high to low
object QueryOrdering extends Ordering[Index] {
  def compare(a:Index, b:Index) = a.value compare b.value
}


/// Data structure (circular array) for finding minimum and maximum for LB_Keogh envolop
class deque(val cap:Int)
{
 var dq = new Array[Int](cap)
 var capacity:Int=cap
 var size:Int=0
 var f:Int=0
 var r:Int=cap-1

 /// Insert to the queue at the back
 def push_back(v:Int):Unit=
 {
  dq(this.r)=v
  r-=1
  if(r<0)
   r=capacity-1
  size+=1
 }

 /// Delete the current (front) element from queue
 def pop_front():Unit=
 {
  f=f-1
  if(f<0)
   f=capacity-1
  size=size-1
 }
/// Delete the last element from queue
 def pop_back():Unit=
 {
  r=(r+1)%capacity
  size=size-1
 }
/// Get the value at the current position of the circular queue
def front():Int=
{
 var aux:Int=f-1;
  if(aux<0)
  aux=capacity-1
  return dq(aux)
}

/// Get the value at the last position of the circular queueint back(struct deque *d)
 def back():Int=
{
 var aux:Int=(r+1)%capacity
 return dq(aux)
}
/// Check whether or not the queue is empty
def empty():Boolean=
{
 return (size==0)
}
}


object SimpleApp
 {
    def dist(x: Double, y: Double) :Double = 
    {
      (x-y) * (x-y)
    }
    def min(x: Double, y: Double) :Double = 
    {
      if (x >y) y else x
    }
    
    def max(x: Double, y: Double) :Double = 
    {
      if (x >y) x else y
    }
    
    def min(x: Int, y: Int) :Int = 
    {
      if (x >y) y else x
    }
    
    def max(x: Int, y: Int) :Int = 
    {
      if (x >y) x else y
    }
/// Finding the envelop of min and max value for LB_Keogh
/// Implementation idea is intoruduced by Danial Lemire in his paper
/// "Faster Retrieval with a Two-Pass Dynamic-Time-Warping Lower Bound", Pattern Recognition 42(9), 2009.
    
 def lower_upper_lemire(t:Array[Double],len:Int,r:Int,l:Array[Double],u:Array[Double]):Unit=
{
 val du=new deque(2*r+2)
 val dl=new deque(2*r+2)
 var i=0
 du.push_back(0)
 dl.push_back(0)

 for(i: Int  <- 1 to len-1)
 {
  if(i>r)
  {
   u(i-r-1)=t(du.front())
   l(i-r-1)=t(dl.front())
  }
  if (t(i) > t(i-1))
  {
   du.pop_back()
    while (!du.empty() && t(i) > t(du.back())) 
      du.pop_back()
  }
  else
  {
    dl.pop_back()
      while (!dl.empty() && t(i) < t(dl.back()))
        dl.pop_back()
  }
   du.push_back(i)
   dl.push_back(i)
   if (i == 2 * r + 1 + du.front())
     du.pop_front()
   else if (i == 2 * r + 1 + dl.front())
     dl.pop_front()
  }
  for(i:Int  <- len to (len+r))
  {
   u(i-r-1) = t(du.front())
        l(i-r-1) = t(dl.front())
        if (i-du.front() >= 2 * r + 1)
            du.pop_front()
        if (i-dl.front() >= 2 * r + 1)
 dl.pop_front()
  }
}

/// LB_Keogh 1: Create Envelop for the query
/// Note that because the query is known, envelop can be created once at the begenining.
///
/// Variable Explanation,
/// order : sorted indices for the query.
/// uo, lo: upper and lower envelops for the query, which already sorted.
/// t     : a circular array keeping the current data.
/// j     : index of the starting location in t
/// cb    : (output) current bound at each position. It will be used later for early abandoning in DTW.

def lb_keogh_cumulative(order:Array[Int],t:Array[Double], uo:Array[Double],lo:Array[Double],cb:Array[Double],j:Int,len:Int,mean:Double,std:Double, best_so_far:Double): Double=
{
var lb:Double=0
var x:Double=0
var d:Double=0
var i:Int=0
 while(lb<best_so_far && i<len)
 {
    x = (t((order(i)+j)) - mean) / std
    d = 0
        if (x > uo(i))
            d = dist(x,uo(i))
        else if(x < lo(i))
            d = dist(x,lo(i))
        lb =lb+ d
        cb(order(i)) = d
        i=i+1

 }
 return lb
}
/// Calculate quick lower bound
/// Usually, LB_Kim take time O(m) for finding top,bottom,fist and last.
/// However, because of z-normalization the top and bottom cannot give siginifant benefits.
/// And using the first and last points can be computed in constant time.
/// The prunning power of LB_Kim is non-trivial, especially when the query is not long, say in length 128.  
  def lb_kim_hierarchy(t :Array[Double], q :Array[Double],j: Int ,m :Int, mean :Double,std :Double,bsf :Double):Double =
    {
    	var d =0.0
    	val x0 = (t(j) -mean)/std
    	val y0 = (t(m-1 +j)-mean)/std
    	var lb = dist(x0,q(0)) + dist(y0,q(m-1))
    	if (lb >= bsf ) return  lb
    	
    	val x1 = (t(j+1) -mean)/std
    	d = min(dist( x1,q(0) ),dist( x0,q(1)))
    	d = min(d, dist(x1, q(1)))
    	lb +=d
    	if (lb >= bsf ) return lb
    	
    	val y1 = (t(m - 2 + j) - mean) / std
        d = min(dist(y1, q(m - 1)), dist(y0, q(m - 2)))
        d = min(d, dist(y1, q(m - 2)))
        lb += d
        if (lb >= bsf) return lb

    	val x2 = (t(j + 2) - mean) / std
	d = min(dist(x0, q(2)), dist(x1, q(2)))
        d = min(d, dist(x2, q(2)))
	d = min(d, dist(x2, q(1)))
	d = min(d, dist(x2, q(0)))
	lb += d
	if (lb >= bsf) return lb
	
        val y2 = (t(m - 3 + j) - mean) / std
	d = min(dist(y0, q(m - 3)), dist(y1, q(m - 3)))
	d = min(d, dist(y2, q(m - 3)))
	d = min(d, dist(y2, q(m - 2)))
	d = min(d, dist(y2, q(m - 1)))
	lb += d
	lb
    }
    
/// Calculate Dynamic Time Wrapping distance
/// A,B: data and query, respectively
/// r  : size of Sakoe-Chiba warpping band    
    def dtw(A : Array[Double], B : Array[Double], m : Int, r :Int, bsf : Double): Double = 
    {
      val size = 2*r+1
      var cost = Array.fill[Double](size)(1e20)
      var cost_prev = Array.fill[Double](size)(1e20)
      var k = 0
      var j = 0 
      var i = 0
      var x : Double = 0.0
      var y : Double = 0.0
      var z : Double = 0.0
      var count = 0
      for( i: Int <- 0 until m)
      {
    	k =  max(0,r-i)
    	var min_cost = 1e20
    	
        for (j :Int <- max(0,i-r) to min(m-1,i+r))
        {
          if(i==0 && j == 0)
          {
            cost(k) = dist(A(0),B(0))
            min_cost =  cost(k)
            k+=1
            
          }
          else
          {
            
            if((j-1 < 0) ||( k-1 < 0 ))
            {
              y = 1e20
            }else 
            {
              y = cost(k-1)
            }
            
            if((i-1 < 0) || (k+1 > 2*r))
            {
              x =1e20
            }else
            {
              x = cost_prev(k+1)
            }
            
            if((i-1 < 0) || (j-1 <0))
            {
              z =1e20
            }else
            {
              z = cost_prev(k)
            }
            count+=1
            cost(k) = min(min(x,y),z) + dist(A(i),B(j))
            if (cost(k) < min_cost){
              min_cost = cost(k)
            }
            k +=1
            
          }
        }
    	if(i+ r < m-1 && min_cost >=bsf)
    	  return min_cost
    	val cost_tmp = cost
    	cost = cost_prev
    	cost_prev = cost_tmp
      }
      k-=1
      cost_prev(k)
    }
    
///Method for computing the BSF and location 
  def process(ind:Int,data: Array[Double], query: Array[Double], m :Int, r :Int):(Double, Int,Int)=
    {
      var ex = 0.0
      var ex2 = 0.0
      var mean = 0.0
      var std = 1.0
      var j = 0
      var lb_kim = 1e20
      var lb_k=1e20
      var bsf = 1e20
      var distance = 1e20
      var loc = 0
      var t = Array.fill[Double](2 *m)(0)
      var tz = Array.fill[Double](m)(0)
      var l=Array.fill[Double](m)(0)
      var u= Array.fill[Double](m)(0)
     
      var socket = new Socket("192.168.0.4", 8008)
      var in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
      var out = new PrintWriter(socket.getOutputStream(), true)
      out.println("1e20")
      bsf =  in.readLine().toDouble
      lower_upper_lemire(query,m,r,l,u)
      var qo= Array.fill[Double](m)(0)   
      var uo= Array.fill[Double](m)(0)
      var lo=Array.fill[Double](m)(0)
      var order= Array.fill[Int](m)(0)
      var Q_tmp=new Array[Index](m)
      var cb=Array.fill[Double](m)(0)
      var cb1=new Array[Double](m)
    
      var i=0
       for(i:Int <- 0 to m-1)
       {
         Q_tmp(i)=new Index(query(i),i)
       }    
      Sorting.quickSort(Q_tmp)(QueryOrdering)

       for(i:Int <- 0 to m-1)
       {
        var o:Int = Q_tmp(i).index
        order(i) = o
        qo(i) = query(o)
        uo(i) = u(o)
        lo(i)= l(o)
       }    

      for(i <- 0 until  data.length)
      {
        ex += data(i)
        ex2 += data(i) * data(i)
        t(i%m) = data(i)
        t(i%m + m) = data(i)
        if(i >= m-1)
        {
          mean =  ex/m
          std = ex2/m
          std = math.sqrt(std-mean*mean)
          j = (i+1)%m
          lb_kim = lb_kim_hierarchy(t, query, j, m, mean, std, bsf)
          if(lb_kim < bsf)
          {
            lb_k=lb_keogh_cumulative(order, t, uo, lo, cb1, j, m, mean, std, bsf)
           if(lb_k< bsf)
            {
            for(k <-0 until m)
            {
             tz(k) = (t(k+j) - mean)/std
            }
     
            distance = dtw(tz, query, m, r, bsf)
            if(distance < bsf)
            {
              bsf = distance
              loc =i
            } 
          }
         } 
        ex -= t(j);
        ex2 -= t(j)*t(j);
        }

      }
      socket.close
      socket = new Socket("192.168.0.4", 8008)
      in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
      out = new PrintWriter(socket.getOutputStream(), true)
      out.println(""+bsf)
      socket.close
      return (bsf,loc,ind)
    }
    
  
    def main(args: Array[String])
  {
      	System.setProperty("spark.executor.memory", "2g")
    	System.setProperty("spark.default.parallelism","20")
    	System.setProperty("spark.scheduler.mode","FAIR")
   	System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	System.setProperty("spark.kryo.registrator", "MyRegistrator")

	val sc = new SparkContext("spark://192.168.0.4:7077", "Simple App", "/home/sparkuser/spark",
        List("hdfs://sparkpesit3:54310/simple-project_2.10-1.0.jar"))
        println(args.mkString("")) 
        val dRDD = sc.textFile("hdfs://sparkpesit3:54310/Data/in*",100)
      	val qp = Source.fromFile(args(0)).mkString("")
      	val query =  qp.trim().replaceAll("""\s+""", " ").split(' ').map(_.toDouble)
      	var ex = 0.0
      	var ex2 = 0.0
      	var std = 0.0
      	var mean = 0.0
      	val m = args(1).toInt
      	var r = (m * args(2).toDouble).toInt
      	for (i <-0 until m)
      {
        ex += query(i)
        ex2 += query(i)*query(i)
      }
      mean = ex/m
      std = ex2/m
      std = math.sqrt(std - mean*mean)
      val query_n = query.map(x => (x-mean)/std)
      val query_norm=sc.broadcast(query_n)
      var ind:Int=0
      
      val res=dRDD.mapPartitionsWithIndex{(ind,iter) => iter.map(x => process(ind,x.trim().split(' ').map(_.toDouble),query_norm.value,m,r))}.reduce((a, b) => if (a._1 < b._1) a else b)

      var idx:Int=0
      var orig_loc:Int=0
      val points=args(3).toInt
     
      var finalbsf = res._1
      var finallocation = res._2 + ((res._3-1)*(points)) + points -(2*m)+1     
        
      println("Distance :" + math.sqrt(finalbsf))
      println("Location :" + finallocation)
    }

}
