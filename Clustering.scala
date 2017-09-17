
val movies = sc.textFile("/usr/local/spark/Sugu/Dataset/ml-100k/u.item")
println(movies.first)

val genres = sc.textFile("/usr/local/spark/Sugu/Dataset/ml-100k/u.genre")
genres.take(5).foreach(println)

val genreMap = genres.filter(!_.isEmpty).map(line => line.split("\\|")).map(array => (array(1), array(0))).collectAsMap
println(genreMap)


val titlesAndGenres = movies.map(_.split("\\|")).map { array =>
	val genres = array.toSeq.slice(5, array.size)
	val genresAssigned = genres.zipWithIndex.filter { case (g, idx) => 
		g == "1" 
	}.map { case (g, idx) => 
		genreMap(idx.toString) 
	}
	(array(0).toInt, (array(1), genresAssigned))
}
println(titlesAndGenres.first)

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
val rawData = sc.textFile("/usr/local/spark/Sugu/Dataset/ml-100k/u.data")
val rawRatings = rawData.map(_.split("\t").take(3))
val ratings = rawRatings.map{ case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
ratings.cache
val alsModel = ALS.train(ratings, 50, 10, 0.1)


import org.apache.spark.mllib.linalg.Vectors
val movieFactors = alsModel.productFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
val movieVectors = movieFactors.map(_._2)
val userFactors = alsModel.userFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
val userVectors = userFactors.map(_._2)


import org.apache.spark.mllib.linalg.distributed.RowMatrix
val movieMatrix = new RowMatrix(movieVectors)
val movieMatrixSummary = movieMatrix.computeColumnSummaryStatistics()
val userMatrix = new RowMatrix(userVectors)
val userMatrixSummary = userMatrix.computeColumnSummaryStatistics()
println("Movie factors mean: " + movieMatrixSummary.mean)
println("Movie factors variance: " + movieMatrixSummary.variance)
println("User factors mean: " + userMatrixSummary.mean)
println("User factors variance: " + userMatrixSummary.variance)

import org.apache.spark.mllib.clustering.KMeans
val numClusters = 5
val numIterations = 10
val numRuns = 3
val movieClusterModel = KMeans.train(movieVectors, numClusters, numIterations, numRuns)

val movieClusterModelConverged = KMeans.train(movieVectors, numClusters, 100)

val userClusterModel = KMeans.train(userVectors, numClusters, numIterations, numRuns)


val movie1 = movieVectors.first
val movieCluster = movieClusterModel.predict(movie1)
println(movieCluster)

val predictions = movieClusterModel.predict(movieVectors)
println(predictions.take(10).mkString(","))

import breeze.linalg._
import breeze.numerics.pow
def computeDistance(v1: DenseVector[Double], v2: DenseVector[Double]): Double = pow(v1 - v2, 2).sum


val titlesWithFactors = titlesAndGenres.join(movieFactors)
val moviesAssigned = titlesWithFactors.map { case (id, ((title, genres), vector)) => 
	val pred = movieClusterModel.predict(vector)
	val clusterCentre = movieClusterModel.clusterCenters(pred)
	val dist = computeDistance(DenseVector(clusterCentre.toArray), DenseVector(vector.toArray))
	(id, title, genres.mkString(" "), pred, dist) 
}
val clusterAssignments = moviesAssigned.groupBy { case (id, title, genres, cluster, dist) => cluster }.collectAsMap 

for ( (k, v) <- clusterAssignments.toSeq.sortBy(_._1)) {
	println(s"Cluster $k:")
	val m = v.toSeq.sortBy(_._5)
	println(m.map { case (id,title, genres, _, d) => (id,
title, genres, d) }.mkString("\n")) 
	println("=====\n")
}


