import com.sonatel.parc.datastream.LoadFile
import com.sonatel.parc.session.CreateSession
import org.apache.spark.sql.functions.lit


object ParcOM extends App {

  def getActiveCustomers(path: String, date: String) = {
    val spark = CreateSession.spark
    import spark.implicits._

    val users = Sico.enrichUsers()
    val msisdnUsers = users.select("msisdn").collect().map(_(0)).toList
    val UsersInParcs = Parc.getUsersInParcs(path, date).select("msisdn").collect().map(_(0)).toList

    // This dataFrame will contains msisdn,grade_name,Famprod,Formule and parc_actif_jour of OM Users
    var df =  Seq.empty[(String, String, String, String, Integer)]
      .toDF("msisdn", "grade_name", "Famprod", "Formule", "parc_actif_jour")
    for (i <-0 to msisdnUsers.length -1) {
      val msisdn = msisdnUsers(i).toString
      var intOut = 0
      if (UsersInParcs.contains(msisdn)) {  intOut = 1  }
      val parcDF = users.select("*").filter(users("msisdn") === msisdn)
        .withColumn("parc_actif_jour", lit(intOut))
      df = parcDF.union(df)
    }
    // Join the df with all data of d_last_transaction
    val df_last_transaction = DateLastTransaction.getDateLastTransaction()
    df = df.join(df_last_transaction, df("msisdn") === df_last_transaction("msisdn"), "inner")
      .drop(df_last_transaction("msisdn"))
    //df.show()
    df.coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .csv("src/main/resources/ActiveCustomers")
    spark.stop()
  }
  //getActiveCustomers("src/main/resources/Parc2","2020-02-01")

  /*
    Function used to give all active customers by service in the parc OM
   */
  def getActiveCustomersByService(path: String, date: String) = {
    val spark = CreateSession.spark
    import spark.implicits._

    val users = Sico.enrichUsers()
    val msisdnUsers = users.select("msisdn").collect().map(_(0)).toList
    val UsersInParcs = Parc.getUsersInParcs(path, date).select("*")

    var dfTotal =  Seq.empty[(String, String, String, String, String, Integer)]
      .toDF("msisdn", "transaction_tag", "grade_name", "Famprod", "Formule", "parc_actif_service_jour")
    for (i <-0 to msisdnUsers.length -1) { // Test for 3 users
      val msisdn = msisdnUsers(i).toString
      val dataUser = users.select("grade_name", "Famprod", "Formule")
        .filter($"msisdn" === s"$msisdn")
      val gradeName = dataUser.select("grade_name").first().toString()
      val Famprod = dataUser.select("Famprod").first().toString()
      val Formule = dataUser.select("Formule").first().toString()

      var df = UsersInParcs.select("msisdn", "transaction_tag")
        .filter($"msisdn" === s"$msisdn")
        .withColumn("grade_name", lit(gradeName))
        .withColumn("Famprod", lit(Famprod))
        .withColumn("Formule", lit(Formule))
        .withColumn("parc_actif_service_jour", lit(1))
      // Select parc present in the dataFrame
      val parcsUser = df.select("transaction_tag").collect().map(_(0)).toList
      val allParcs = Parc.getAllParcs(path)
      val diffParcsUser = allParcs.diff(parcsUser)
      diffParcsUser.foreach(transaction_tag => {
        val row = users.select("*").filter($"msisdn" === s"$msisdn")
          .withColumn("transaction_tag", lit(s"$transaction_tag"))
          .withColumn("parc_actif_service_jour", lit(0))
        df = row.select("msisdn", "transaction_tag", "grade_name", "Famprod",
        "Formule", "parc_actif_service_jour").union(df)
        df.show()
      })
      dfTotal = df.union(dfTotal)
    }
    // Join dfTotal dataFrame with all data of the table historique
    val df_historique = LoadFile.load("historiqueTransaction.csv","src/main/resources/Test/")
    dfTotal = dfTotal.join(df_historique,
      dfTotal("msisdn") === df_historique("msisdn") && dfTotal("transaction_tag") === df_historique("regroupement"),
    "left")
        .drop(df_historique("msisdn"))
        .drop(df_historique("regroupement"))
    //dfTotal.show()
    dfTotal.coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .csv("src/main/resources/ActiveCustomersByService")
    spark.stop()
  }
  getActiveCustomersByService("src/main/resources/Parc2","2020-02-01")

}
