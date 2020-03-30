import com.sonatel.parc.datastream.LoadFile
import com.sonatel.parc.session.CreateSession

object Sico {

  // This function is used to enrich the table om_subscribers with sico table
  def enrichUsers() = {
    val spark = CreateSession.spark
    import spark.implicits._
    import org.apache.spark.sql.functions.split

    val omUsers = LoadFile.load("om_subscribers.csv", "src/main/resources/Test/")
    val sicoUsers = LoadFile.load("sico1.csv")
      .withColumn("newColumn", split($"type_contrat", "\\-"))
      .select(
        $"nd",
              $"newColumn".getItem(0).as("Famprod"),
              $"newColumn".getItem(1).as("Formule")
      )
      .drop($"type_contrat")

    val enrich = omUsers.join(sicoUsers, omUsers("MSISDN") === sicoUsers("nd"), "inner")
      .drop("nd")
      .withColumnRenamed("MSISDN", "msisdn")
      .withColumnRenamed("USER_GRADE_NAME", "grade_name")
    enrich
  }
  //enrichUsers().select("*").show()

}
