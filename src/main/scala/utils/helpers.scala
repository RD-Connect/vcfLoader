package utils

object helpers {
  def chromStrToInt(chrom:String)={
    chrom match {
      case "MT" =>"23"
      case "X" => "24"
      case "Y" => "25"
      case _ => chrom
    }
  }


}