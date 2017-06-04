/*
 * Incremental calculation for variance.
 */
package org.apache.spark.sql

object commonMath {

  def CNDF(x: Double): Double = {
    var mX = x
    val neg = if (mX < 0d) 1
    else 0
    if (neg == 1) mX = mX * -1d

    val k = 1d / (1d + 0.2316419 * mX)

    var y = ((((1.330274429 * k - 1.821255978) * k + 1.781477937) * k - 0.356563782)
      * k + 0.319381530) * k
    y = 1.0 - 0.398942280401 * Math.exp(-0.5 * mX * mX) * y

    (1d - neg) * y + neg * (1d - y)
  }

  /**
    * Compute the 'p'-th quantile for the "standard normal distribution" function.
    *
    * @param p the p-th quantile, e.g., .95 (95%)
    * @return the 'p'-th quantile for the "standard normal distribution" function.
    *         This function returns an approximation of the "inverse" cumulative
    *         standard normal distribution function, i.e., given 'p', it returns an
    *         approximation to the 'x' satisfying
    *         <p>
    *         p = F(x) = P(Z <= x)
    *         <p>
    *         where Z is a random variable from the standard normal distribution.
    *         The algorithm uses a minimax approximation by rational functions and the
    *         result has a relative error whose absolute value is less than 1.15e-9.
    */
  def normalInv(p: Double = .95): Double = {
    // Coefficients in rational approximations
    val a = Array(-3.969683028665376e+01, 2.209460984245205e+02,
      -2.759285104469687e+02, 1.383577518672690e+02,
      -3.066479806614716e+01, 2.506628277459239e+00)

    val b = Array(-5.447609879822406e+01, 1.615858368580409e+02,
      -1.556989798598866e+02, 6.680131188771972e+01,
      -1.328068155288572e+01)

    val c = Array(-7.784894002430293e-03, -3.223964580411365e-01,
      -2.400758277161838e+00, -2.549732539343734e+00,
      4.374664141464968e+00, 2.938163982698783e+00)

    val d = Array(7.784695709041462e-03, 3.224671290700398e-01,
      2.445134137142996e+00, 3.754408661907416e+00)

    // Define break-points
    val plow = 0.02425
    val phigh = 1 - plow

    // Rational approximation for lower region:
    if (p < plow) {
      val q = math.sqrt(-2 * math.log(p))
      return (((((c(0) * q + c(1)) * q + c(2)) * q + c(3)) * q + c(4)) * q + c(5)) /
        ((((d(0) * q + d(1)) * q + d(2)) * q + d(3)) * q + 1)
    }

    // Rational approximation for upper region:
    if (phigh < p) {
      val q = math.sqrt(-2 * math.log(1 - p))
      return -(((((c(0) * q + c(1)) * q + c(2)) * q + c(3)) * q + c(4)) * q + c(5)) /
        ((((d(0) * q + d(1)) * q + d(2)) * q + d(3)) * q + 1)
    }

    // Rational approximation for central region:
    val q = p - 0.5
    val r = q * q
    (((((a(0) * r + a(1)) * r + a(2)) * r + a(3)) * r + a(4)) * r + a(5)) * q /
      (((((b(0) * r + b(1)) * r + b(2)) * r + b(3)) * r + b(4)) * r + 1)
  }

  def main(args: Array[String]): Unit = {
    implicit val arrayToSamples = (values: Array[Double]) => NumberSamples(values)

    val historicalSamples = Array(1.5d, 3.4d, 7.8d, 11.6d)
    val deltaSamples = Array(9.4d, 4.2d, 35.6d, 77.9d)

    var deltaVar =
      historicalSamples.measures.appendDelta(deltaSamples.measures).variance

  }

  def calcConfidence(errorBound: Double,
                     samplesCount: Long,
                     T_n_2: Double): Double = {
    2 * CNDF((errorBound * math.sqrt(samplesCount)) / math.sqrt(T_n_2)) - 1
  }

  def calcErrorBound(confidence: Double,
                     samplesCount: Long,
                     T_n_2: Double): Double = {
    val z_p = normalInv((1 + confidence) / 2)
    math.sqrt(((z_p * z_p) * T_n_2) / samplesCount)
  }
}

case class DeltaVarianceMeasures(n: Int, sum: Double, variance: Double) {
  def mAvg: Double = sum / n

  def appendDelta(delta: DeltaVarianceMeasures): DeltaVarianceMeasures = {
    val newN = this.n + delta.n
    val newSum = this.sum + delta.sum
    val newAvg = newSum / newN

    def partial(m: DeltaVarianceMeasures): Double = {
      val deltaAvg = newAvg - m.mAvg
      m.n * (m.variance + deltaAvg * deltaAvg)
    }

    val newVariance = (partial(this) + partial(delta)) / newN

    DeltaVarianceMeasures(newN, newSum, newVariance)
  }
}

case class NumberSamples(values: Seq[Double]) {
  def measures: DeltaVarianceMeasures = {
    if (values == null || values.isEmpty) {
      DeltaVarianceMeasures(0, 0d, 0d)
    }
    else {
      DeltaVarianceMeasures(values.length, values.sum, variance)
    }
  }

  private def variance: Double = {
    val n = values.length
    val avg = values.sum / n
    values.foldLeft(0d) { case (sum, sample) =>
      sum + (sample - avg) * (sample - avg)
    } / n
  }
}