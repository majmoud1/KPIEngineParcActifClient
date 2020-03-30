import com.sonatel.parc.datastream.LoadFile
import com.sonatel.parc.session.CreateSession

object DateLastTransaction extends App {
  /*
    This function is used to get the last transaction date (max d_last transaction)
    and the min of anciennete transaction (min anciennete_transaction) for the Parc client actif
   */
  def getDateLastTransaction () = {
    val spark = CreateSession.spark
    val df_historique = LoadFile.load("historiqueTransaction.csv","src/main/resources/Test/")
    df_historique.createOrReplaceTempView("historique")
    val df = spark.sql(
      """SELECT DISTINCT msisdn,
        |MAX(d_last_transaction) AS d_last_transaction,
        |MIN(anciennete_transaction_service) AS anciennete_transaction_service
        |FROM historique
        |GROUP BY msisdn""".stripMargin)
    df
  }
  //getDateLastTransaction


}
