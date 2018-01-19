package mob

import java.util
import java.util.UUID

import org.apache.solr.client.solrj.impl.{CloudSolrServer, HttpClientUtil, LBHttpSolrServer}
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hello world!
  *
  */
object App {
  /**
    * 检查hive数据源可用性
    *
    * @param dbName
    * @param tableName
    * @param hiveContext
    * @return
    */
  def checkTable(dbName: String, tableName: String, hiveContext: HiveContext): Boolean = {
    if (tableName.isEmpty || dbName.isEmpty) {
      return false;
    }
    hiveContext.sql(s"use ${dbName}")
    return hiveContext.tableNames().toSet.contains(tableName)
  }

  /**
    * 加载hive数据
    *
    * @param tableName
    * @param dbName
    */
  def loadFromHive(dbName: String, tableName: String): DataFrame = {
    val conf = new SparkConf().setAppName("dfd")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc = sc)
    if (checkTable(dbName, tableName, hiveContext)) {
      return hiveContext.sql(s"select app_id,icon,appname,app_category_id,publisher,rank,channel_id,family  from ${dbName}.${tableName}")
    } else {
      throw new Exception(s"hive数据源${dbName}.${tableName}不可用！")
    }

  }

  def main(args: Array[String]): Unit = {

    val tableName = "app_publisher_search_sdk_full"
    val dbName = "rp_mobeye_app360"
    val solrTab = "app_publisher_search_sdk_full"
    val df = loadFromHive(dbName, tableName)
    df.foreachPartition(list => {
      val batchSize = 10000;
      val httpColent = HttpClientUtil.createClient(null)
      val lBHttpSolrClient = new LBHttpSolrServer(httpColent)
      val sc1 = new CloudSolrServer("bd15-002:2181,bd15-003:2181,bd15-004:2181/solr", lBHttpSolrClient)
      sc1.setDefaultCollection(solrTab)
      sc1.setZkClientTimeout(60000)
      var datas = new util.LinkedList[SolrInputDocument]();
      while (list.hasNext) {
        val x = list.next;
        val doc = new SolrInputDocument();
        doc.addField("id", UUID.randomUUID().toString)
        doc.addField("app_id", x.getString(0));
        doc.addField("icon", x.getString(1));
        doc.addField("appname", x.getString(2));
        doc.addField("app_category_id", x.getString(3));
        doc.addField("publisher", x.getString(4));
        doc.addField("rank", x.getLong(5));
        doc.addField("channel_id", x.getInt(6));
        doc.addField("family", x.getInt(7));
        datas.add(doc);
        if (datas.size() == batchSize) {
          try {
            sc1.add(datas);
            sc1.commit();
          } catch {
            case e: Exception =>
              e.printStackTrace();
          } finally {
            datas.clear();
          }
        }
      }
      if (datas.size > 0) {
        sc1.add(datas)
        sc1.commit();
      }
      sc1.shutdown()
    })

  }

  def test(): Unit = {
    val httpColent = HttpClientUtil.createClient(null)
    val lBHttpSolrClient = new LBHttpSolrServer(httpColent)
    val sc1 = new CloudSolrServer("bd15-002:2181,bd15-003:2181,bd15-004:2181/solr", lBHttpSolrClient)
    sc1.setDefaultCollection("mobeye_device_info_search_sdk_full")
    sc1.setZkClientTimeout(60000)
    val doc = new SolrInputDocument()
    doc.addField("id", UUID.randomUUID().toString)
    doc.addField("app_id", "app11")
    doc.addField("icon", "icon1")
    doc.addField("appname", "appname1")
    doc.addField("app_category_id", "app_a_id_1")
    doc.addField("publisher", "pubulisher1")
    doc.addField("rank", 1)
    doc.addField("channel_id", 1)
    doc.addField("family", 1)
    //    doc.addField("par_channel","par_channel11")
    sc1.add(doc)
    sc1.commit()
    //                client.add(doc)
  }
}
