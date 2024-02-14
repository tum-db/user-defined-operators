#include <atomic>
#include <random>
//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
/// A generator for random 2D points that lie on the curve given by:
///
/// y = a + bx + cx^2 + e
///
/// Where a, b, and c are given in the constructor and e is a randomly
/// generated error value that is normally distributed with mean 0 and
/// variance (a + b + c)^2.
/// x will be chosen randomly uniformly distributed in [0, 100].
class CreateRegressionPoints : public udo::UDOperator {
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
   using InputTuple = udo::EmptyTuple;
   struct OutputTuple {
      /// The x value
      double x;
      /// The y value
      double y;
   };

   /// Constructor
   CreateRegressionPoints(double a, double b, double c, uint64_t numPoints)
      : a(a), b(b), c(c), numPoints(numPoints) {}

   /// Produce the output
   bool process(udo::ExecutionState executionState) {
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
         emit<CreateRegressionPoints>(executionState, {x, y});
      }

      return false;
   }
};
//---------------------------------------------------------------------------
#ifdef WASMUDO
// plugin-wasmudo -generate-cxx-header -no-destroy -no-accept CreateRegressionPoints 40 8 f64,f64,f64,i64 '' 'x f64,y f64' > wasmudo_create_regression_points.hpp
#include "wasmudo_create_regression_points.hpp"
#endif
//---------------------------------------------------------------------------
