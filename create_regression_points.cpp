#include <atomic>
#include <random>
//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
/// The output of this operator
struct Output {
   /// The x value
   double x;
   /// The y value
   double y;
};
//---------------------------------------------------------------------------
/// A generator for random 2D points that lie on the curve given by:
///
/// y = a + bx + cx^2 + e
///
/// Where a, b, and c are given in the constructor and e is a randomly
/// generated error value that is normally distributed with mean 0 and
/// variance (a + b + c)^2.
/// x will be chosen randomly uniformly distributed in [0, 100].
class CreateRegressionPoints : public udo::UDOperator<udo::EmptyTuple, Output> {
   private:
   /// The parameter a
   double a;
   /// The parameter b
   double b;
   /// The parameter c
   double c;
   /// The number of points that should be generated
   uint64_t numPoints;
   /// The counter for the threads that generate the points
   atomic<uint64_t> pointsCounter = 0;

   public:
   /// Constructor
   CreateRegressionPoints(double a, double b, double c, uint64_t numPoints)
      : a(a), b(b), c(c), numPoints(numPoints) {}

   /// Produce the output
   bool postProduce(LocalState& /*localState*/) {
      auto firstIndex = pointsCounter.fetch_add(10000);
      if (firstIndex >= numPoints)
         return true;

      uint64_t seed = 42 + firstIndex;
      mt19937_64 gen(seed);

      uniform_real_distribution xDist(0.0, 100.0);
      double stddev = a + b + c;
      normal_distribution eDist(0.0, stddev);

      for (uint64_t i = 0; i < 10000 && firstIndex + i < numPoints; ++i) {
         double x = xDist(gen);
         double e = eDist(gen);
         double y = a + b * x + c * x * x + e;
         produceOutputTuple({x, y});
      }

      return false;
   }
};
//---------------------------------------------------------------------------
