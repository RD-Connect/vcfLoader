package Elastic


import org.elasticsearch.common.settings.Settings
import org.elasticsearch.client.transport.TransportClient
import play.api.Play.current
import play.api.libs.ws._
//import play.api.libs.ws.ning.NingAsyncHttpClientConfigBuilder
import scala.concurrent.Future
import play.api.libs.json._
import scala.concurrent.Await
import scala.concurrent.duration._
import play.api._

// Instantiation of the client
// In a real-life application, you would instantiate one, share it everywhere,
// and call wsClient.close() when you're done

object Data {
  def mapping(sparkContext: org.apache.spark.SparkContext, index_name: String, version: String, host: String, port: Int, action: String):String = {
    //val settings = Settings.s   .put("cluster.name", "myClusterName").build()

    val env = Environment(new java.io.File("."), this.getClass.getClassLoader, Mode.Dev)
    val context = ApplicationLoader.createContext(env)
    val loader = ApplicationLoader(context)
    val app = loader.load(context)
    Play.start(app)
    val wsClient = WS
    var result="OK"
    //val client= TransportClient.builder().settings(settings).build()
    if (action == "create") {
      //import scala.concurrent.ExecutionContext.Implicits._

      val json: JsValue = Json.parse(s"""
      {"settings":{"index":{"number_of_shards":8,"number_of_replicas":0,"refresh_interval":"1000.ms"}}
        ,"mappings":{"$version"
        :{"_all":{"enabled":false},
        "properties":{"chrom":{"type":"integer","index":"not_analyzed"},
        "pos":{"type":"integer","index":"not_analyzed"}
        ,"ref":{"type":"string","index":"no"}
        ,"alt":{"type":"string","index":"no"}
        ,"freqInt":{"type":"float","index":"no"}
        ,"indel":{"type":"string","index":"not_analyzed"}
        ,"effs":{"type":"nested","properties":{"codon_change":{"type":"string","index":"no"}
        ,"amino_acid_change":{"type":"string","index":"no"}
        ,"amino_acid_length":{"type":"string","index":"no"}
        ,"codon_change":{"type":"string","index":"no"}
        ,"effect":{"type":"string","index":"not_analyzed"}
        ,"effect_impact":{"type":"string","index":"not_analyzed"}
        ,"exon_rank":{"type":"string","index":"no"}
        ,"functional_class":{"type":"string","index":"no"}
        ,"gene_coding":{"type":"string","index":"not_analyzed"}
        ,"gene_name":{"type":"string","index":"not_analyzed"}
        ,"transcript_biotype":{"type":"string","index":"not_analyzed"}
        ,"transcript_id":{"type":"string","index":"not_analyzed"}
        }}
        ,"predictions":{"type":"nested",
        "properties":{"cadd_phred":{"type":"float","index":"not_analyzed"}
        ,"gerp_rs":{"type":"string","index":"no"}
        ,"mt":{"type":"string","index":"no"}
        ,"mutationtaster_pred":{"type":"string","index":"not_analyzed"}
        ,"phylop46way_placental":{"type":"string","index":"no"}
        ,"polyphen2_hvar_pred":{"type":"string","index":"not_analyzed"}
        ,"polyphen2_hvar_score":{"type":"string","index":"no"}
        ,"sift_pred":{"type":"string","index":"not_analyzed"}
        ,"sift_score":{"type":"string","index":"no"}
        ,"siphy_29way_pi":{"type":"string","index":"no"}
        ,"UMD":{"type":"string","index":"not_analyzed"}
        ,"clinvar":{"type":"string","index":"no"}
        ,"clinvar_filter":{"type":"string","index":"not_analyzed"}
        ,"clnacc":{"type":"string","index":"no"},
        "rs":{"type":"string","index":"not_analyzed"}
        }},
        "populations":{"type":"nested",
        "properties":{"gp1_afr_af":{"type":"float","index":"no"}
        ,"gp1_asn_af":{"type":"float","index":"no"}
        ,"gp1_eur_af":{"type":"float","index":"no"}
        ,"gp1_af":{"type":"float","null_value":0.0}
        ,"esp6500_aa":{"type":"float","null_value":0.0}
        ,"esp6500_ea":{"type":"float","null_value":0.0}
        ,"exac":{"type":"float","null_value":0.0}
        ,"gmaf":{"type":"float","index":"no"}
        ,"rd_freq":{"type":"float","index":"no"}}}
        ,"samples":{"type":"nested",
        "properties":{"dp":{"type":"float"}
        ,"gq":{"type":"float"}
        ,"ad":{"type":"float"}
        ,"gt":{"type":"string"
        ,"index":"not_analyzed"}
        ,"sample":{"type":"string","index":"not_analyzed"}
        ,"multi":{"type":"string","index":"no"},
        "diploid":{"type":"string","index":"no"}}}}}}}
      """)
      println("before creating indeX")

      Await.result( wsClient.url("http://"+host+":"+port+"/"+index_name).post(json), 5 seconds)

      /*val futureResponse=  wsClient.url("http://localhost:9200/prova2").post(json).map { wsResponse =>
         if (! (200 to 299).contains(wsResponse.status)) {
           sys.error(s"Received unexpected status ${wsResponse.status} : ${wsResponse.body}")
         }
         println(s"OK, received ${wsResponse.body}")
         result="OK"

       }*/
//      wsClient.url("http://playframework.org/").withRequestTimeout(10000 /* in milliseconds */)
       /* .url("http://localhost:9200/_alias")
        .get()
        .map { wsResponse =>
          if (! (200 to 299).contains(wsResponse.status)) {
            sys.error(s"Received unexpected status ${wsResponse.status} : ${wsResponse.body}")
          }
          println(s"OK, received ${wsResponse.body}")
          println(s"The response header Content-Length was ${wsResponse.header("Content-Length")}")
         result
         }*/
        println("after creating indeX")

    }
    if (action == "delete") {
      Await.result( wsClient.url("http://"+host+":"+port+"/"+index_name).delete() , 5 seconds)

      result="OK"
    }
    Play.stop(app)

    //codon_changes twice
    return result
  }
}
