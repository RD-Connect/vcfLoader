/*case class rangeList (start:Int,end:Int,samples:List[String])

def mergegVcf(aiter: Iterator[range], biter: Iterator[range]): Iterator[rangeList] =
{
  var res = List[rangeList]()
  var min :Int= 0
  var Amin:Int =0
  var Bmin :Int= 0
  var Astart,Aend,Aopen,Afinish= false
  var Bstart,Bend,Bopen,Bfinish= false
  var Copen=false
  var Cstart :Int= 0
  var AtoBeOpen=true
  var BtoBeOpen=true
  var Crange:Option[rangeList]=None
  var CStartBoolean=false
  var CEndBoolean=false
  var Acurrent:range= null
  var Bcurrent:range= null
  while (aiter.hasNext || biter.hasNext ||  (Aopen || Bopen) )
  {
    if (aiter.hasNext && AtoBeOpen) {Acurrent= aiter.next; Astart= true; AtoBeOpen=false }
    if (biter.hasNext && BtoBeOpen) {Bcurrent= biter.next; Bstart= true; BtoBeOpen=false }
    if (Astart)
      Amin= Acurrent.start
    else  Amin= Acurrent.end
    if (Bstart)  Bmin= Bcurrent.start
    else Bmin= Bcurrent.end
    if (Afinish) Amin=1000000000
    if (Bfinish) Bmin=1000000000

    min=List(Amin,Bmin).min


    var lista:List[String]=List()
    if (Bopen)
      lista ::=  "2"
    if (Aopen ) lista ::=  "1"
    if (lista.size > 0)
      Crange=Some(rangeList(Cstart,min,lista))
    Cstart=min;
    if (Crange.isDefined){
      res ::= Crange.get
      Crange=None
    }
    if (min==Amin){
      if (Astart) {
        Aopen=true;
        Astart=false
      }
      else {
        Aopen=false
        AtoBeOpen=true
        if (!aiter.hasNext) Afinish=true
      }
    }
    if (min==Bmin) {
      if (Bstart) {
        Bopen=true
        Bstart=false}
      else {
        Bopen=false
        BtoBeOpen=true
        if (!biter.hasNext) Bfinish=true
      }
    }
  }
  res.iterator
}
a.zipPartitions(b)(mergegVcf).collect

*/