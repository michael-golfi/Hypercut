package dbpart

object FlatQ {
    
  def stream(from: Iterator[String]) =    
    from.flatMap(s => {      
      val spl = s.split("\t", 3)
      Some((spl(1), s))      
    })  
}