package examen_estructura

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Examen {

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   * Realiza las siguientes operaciones:
   *
   * Muestra el esquema del DataFrame.
   * Filtra los estudiantes con una calificación mayor a 8.
   * Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    estudiantes.select("nombre").orderBy(col("calificacion").desc)
  }

  /** Ejercicio 2: UDF (User Defined Function)
   * Pregunta: Define una función que determine si un número es par o impar.
   * Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  val udfParidad: UserDefinedFunction = udf((numero: Int) => numero match {
    case n if n % 2 == 0 => "Par"
    case n if n % 2 != 0 => "Impar"
  })

  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
    numeros.withColumn("numero", udfParidad(col("numero")))
  }

  /** Ejercicio 3: Joins y agregaciones
   * Pregunta: Dado dos DataFrames,
   * uno con información de estudiantes (id, nombre)
   * y otro con calificaciones (id_estudiante, asignatura, calificacion),
   * realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    val dfPromedios: DataFrame = calificaciones
      .groupBy("id_estudiante")
      .agg(avg("calificacion").alias("promedio"))
      .join(estudiantes, col("id_estudiante") === col("id"), "inner")
      .select("id_estudiante", "nombre", "promedio")
      .orderBy(col("id_estudiante").asc)
    dfPromedios

  }


  /** Ejercicio 4: Uso de RDDs
   * Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
   */

  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
    // Usando RDD como pedía el ejercicio
    val palabrasRDD = spark.sparkContext.parallelize(palabras) // Creamos RDD de la lista de palabras

    palabrasRDD
      .map(palabra => (palabra,1))  // Transformamos cada palabra como key de un map, con value 1 (luego agregaremos por key)
      .reduceByKey(_ + _)  // Agregamos los values por key, creando las tuplas (palabra, conteo)
  }

  /*def ejercicio4B(palabras: List[String]): Array[(String, Int)] = {
    // Sin Spark (más simple y funciona bien para listas cortas)
    palabras
      .groupBy(identity) //La función de Scala "identity" devuelve el input. Así agrupamos por palabra de forma sencilla.
      .map { case (palabra, lista) => (palabra, lista.size) } //Aquí creamos las tuplas (palabras, conteo)
      .toArray
  }*/


  /**
   * Ejercicio 5: Procesamiento de archivos
   * Pregunta: Carga un archivo CSV que contenga información sobre
   * ventas (id_venta, id_producto, cantidad, precio_unitario)
   * y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */
  def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {
    ventas
      .withColumn("ingreso", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso").alias("ingreso_total"))
      .orderBy("id_producto")
  }

}
