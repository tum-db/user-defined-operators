#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <random>
//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
/// The output of this operator
struct Output {
   /// The x dimension of the point
   double x;
   /// The y dimension of the point
   double y;
   /// The cluster id from the cluster this point belongs to
   uint32_t clusterId;
};
//---------------------------------------------------------------------------
/// A 2D point
struct Point2D {
   /// The x dimension
   double x;
   /// The y dimension
   double y;
};
//---------------------------------------------------------------------------
/// A generator for random 2D Points with a cluster id
class CreatePoints : public udo::UDOperator<udo::EmptyTuple, Output> {
   private:
   /// The fixed cluster centers
   static constexpr array<Point2D, 10> clusterCenters{{
      {0.0, 0.0},
      {40.0, 0.0},
      {0.0, -40.0},
      {-40.0, 0.0},
      {0.0, 40.0},
      {50.0, 44.0},
      {40.0, -80.0},
      {-30.0, -50.0},
   }};

   /// The standard deviations for the clusters
   static constexpr array<double, 10> stdDevs{
      5.0,
      5.0,
      5.0,
      5.0,
      5.0,
      7.0,
      8.0,
      1.0,
   };

   /// The proportion of points that should belong to this cluster
   static constexpr array<double, 10> clusterPointsProportions{
      1.0 / 8,
      1.0 / 8,
      1.0 / 8,
      1.0 / 8,
      1.0 / 8,
      1.0 / 64,
      1.0 / 64 * 15,
      1.0 / 8,
   };

   /// The number of points that should be generated
   uint64_t numPoints;
   /// The next cluster that should be generated
   atomic<uint32_t> nextClusterId = 0;

   public:
   /// Constructor
   explicit CreatePoints(uint64_t numPoints) : numPoints(numPoints) {}

   /// Produce the output
   bool postProduce(LocalState& /*localState*/) {
      auto clusterId = nextClusterId.fetch_add(1);
      if (clusterId >= clusterCenters.size())
         return true;

      uint64_t seed = 42 + clusterId;
      mt19937_64 gen(seed);

      normal_distribution xDist(clusterCenters[clusterId].x, stdDevs[clusterId]);
      normal_distribution yDist(clusterCenters[clusterId].y, stdDevs[clusterId]);

      uint64_t numClusterPoints = ceil(numPoints * clusterPointsProportions[clusterId]);

      for (uint64_t i = 0; i < numClusterPoints; ++i) {
         Output output;
         output.x = xDist(gen);
         output.y = yDist(gen);
         output.clusterId = clusterId;
         produceOutputTuple(output);
      }

      return false;
   }
};
//---------------------------------------------------------------------------
