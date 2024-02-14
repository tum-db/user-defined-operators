#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <random>
#include <span>
#include <utility>
#include <vector>
#ifdef UDO_STANDALONE
#include "standalone_util.hpp"
#include <chrono>
#include <udo/UDOStandalone.hpp>
#endif
//---------------------------------------------------------------------------
#include <udo/ChunkedStorage.hpp>
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
template <typename T1, typename T2>
double distance(const T1& a, const T2& b)
// Calculate the distance between two points
{
   double x = b.x - a.x;
   double y = b.y - a.y;
   // Return the squared euclidian distance
   return x * x + y * y;
}
//---------------------------------------------------------------------------
/// A helper class to to reservoir sampling
template <typename T>
class ReservoirSample {
   private:
   /// The actual sample
   vector<T> sample;
   /// The sample size
   uint64_t limit;
   /// The number of tuples seen for sampling
   uint64_t elementsSeen;
   /// The random engine
   mt19937_64 mt;
   /// The distribution for random numbers
   uniform_real_distribution<double> doubleDist;
   /// The distribution for random slots
   uniform_int_distribution<uint64_t> slotDist;
   /// The number of elements to skip
   uint64_t skip;
   /// The W of Li's algorithm L
   double w;

   public:
   /// Constructor
   ReservoirSample(uint64_t sampleSize, uint64_t seed)
      : sample(sampleSize), limit(sampleSize), elementsSeen(0), mt(seed), doubleDist(0.0, 1.0), slotDist(0, sampleSize - 1) {
      // Calculate initial skip after algorithm l https://doi.org/10.1145/198429.198435
      w = exp(log(doubleDist(mt)) / limit);
      skip = static_cast<uint64_t>(floor(log(doubleDist(mt)) / log(1.0 - w)));
   }

   /// Set the number of tuples that were seen for this sample
   void setElementsSeen(uint64_t n) {
      elementsSeen = n;
   }

   /// Get the sample
   span<T> getSample() {
      return sample;
   }

   /// Get random index for reservoir slot
   uint64_t getRandomSlot() {
      // Calculate next step after algorithm l https://doi.org/10.1145/198429.198435
      if (skip == 0) {
         w *= exp(log(doubleDist(mt)) / limit);
         skip = static_cast<uint64_t>(floor(log(doubleDist(mt)) / log(1.0 - w)));
         return slotDist(mt);
      }
      skip--;
      return limit + skip;
   }

   /// Combine two reservoirs keeping uniformity
   void mergeInto(ReservoirSample& target) {
      if (elementsSeen == 0)
         return;

      if (target.elementsSeen < limit && elementsSeen < limit) {
         // We have two incomplete samples. We just complete the sample of the
         // target by using the samples of the source as individual tuples.
         uint64_t copySamples = min(limit - target.elementsSeen, elementsSeen);
         move(sample.begin(), sample.begin() + copySamples, target.sample.begin() + target.elementsSeen);
         target.elementsSeen += copySamples;
         elementsSeen -= copySamples;

         if (elementsSeen == 0)
            return;
      }

      // If either the source or the target does not have a full sample, we have
      // to special case this to make sure the merged sample is still uniform.
      if (target.elementsSeen < limit || elementsSeen < limit) {
         auto* mergeSource = this;
         auto* mergeTarget = &target;

         // When this operator already has a full sample but the target doesn't,
         // we instead merge the target into the source which makes it easier to
         // keep uniformity.
         if (target.elementsSeen < limit && elementsSeen >= limit) {
            mergeSource = &target;
            mergeTarget = this;
         }

         // Treat the source as individual new tuples and use the regular sampling
         // logic to add them to the target. At this point we know that the target
         // is definitely full.
         // Use algorithm R to merge the remaining tuples
         for (uint64_t i = 0; i < mergeSource->elementsSeen; ++i) {
            auto dist = uniform_int_distribution<uint64_t>(0, mergeTarget->elementsSeen + i);
            auto sampleIndex = dist(mt);
            if (sampleIndex < limit)
               mergeTarget->sample[sampleIndex] = move(mergeSource->sample[i]);
         }

         // If we swapped source and target, we need to copy the samples back to the target.
         if (target.elementsSeen < limit && elementsSeen >= limit)
            move(mergeTarget->sample.begin(), mergeTarget->sample.end(), mergeSource->sample.begin());
      } else {
         // Do a regular merge of two full samples.
         auto dist = uniform_int_distribution<uint64_t>(1, elementsSeen + target.elementsSeen);
         for (auto i = 0u; i < limit; i++)
            if (dist(mt) <= elementsSeen)
               target.sample[i] = move(sample[i]);
      }

      target.elementsSeen += elementsSeen;
   }
};
//---------------------------------------------------------------------------
// The k-means Operator
class KMeans : public udo::UDOperator {
   public:
   struct InputTuple {
      // The x-coordinate
      double x;
      // The y-coordinate
      double y;
      // The payload
      uint64_t payload;

      template <size_t I>
         requires(I < 3)
      auto& get() {
         if constexpr (I == 0)
            return x;
         else if constexpr (I == 1)
            return y;
         else if constexpr (I == 2)
            return payload;
      }
   };
   struct OutputTuple {
      // The x-coordinate
      double x;
      // The y-coordinate
      double y;
      // The payload
      uint64_t payload;
      // The cluster id
      uint16_t clusterId;
   };

   private:
   /// Possible operation types
   enum Operation : uint32_t {
      PrepareInitializeClusters = 0,
      FinishInitializeClusters,
      PrepareAssociatePoints,
      AssociatePoints,
      FinishAssociatePoints,
      PrepareRecalculateMeans,
      RecalculateMeans,
      FinishRecalculateMeans,
      PrepareWriteOutput,
      WriteOutput = extraWorkDone,
   };

   /// The locale state in consume()
   struct ConsumeLocalState {
      /// The tuple storage for this worker.
      udo::ParallelChunkedStorage<OutputTuple>::LocalChunkedStorageRef tuplesRef;
      /// The sample for this worker
      ReservoirSample<OutputTuple*> sample;
      /// The next local state
      ConsumeLocalState* next = nullptr;

      /// Constructor
      ConsumeLocalState(size_t sampleSize, uint64_t seed) : sample(sampleSize, seed) {}
   };

   /// A cluster center
   struct ClusterCenter {
      /// The x coordinate
      double x;
      /// The y coordinate
      double y;
   };

   /// A cluster center that also tracks the number of points per cluster
   struct LocalClusterCenter {
      /// The number of points
      uint64_t numPoints;
      /// The x coordinate
      double x;
      /// The y coordinate
      double y;
   };

   /// One element of the linked list that contains all local cluster centers
   /// in recalculateMeans
   struct LocalClustersEntry {
      /// The cluster centers
      vector<LocalClusterCenter> centers;
      /// The next entry
      LocalClustersEntry* next = nullptr;
   };

   /// The number of clusters
   static constexpr unsigned numClusters = 8;

   /// The storage for all tuples
   udo::ParallelChunkedStorage<OutputTuple> tuples;
   /// The total number of tuples
   size_t numTuples = 0;
   /// The local states in consume
   atomic<ConsumeLocalState*> consumeLocalStateList = nullptr;
   /// The cluster centers
   vector<ClusterCenter> centers;
   /// The linked list of local cluster centers used in recalculateMeans
   atomic<LocalClustersEntry*> localClusterCentersList = nullptr;
   /// The mutex flag for the prepare steps of the operations
   atomic_flag prepareMutex = false;
   /// The number of iterations
   unsigned numIterations = 0;
   /// The number of points that changed their cluster
   atomic<size_t> numChangedPoints;
   /// The parallel iterator that is used to iterate through the tuples.
   decltype(tuples.parallelIter()) tuplesIter;

   public:
   /// Constructor
   KMeans() {
      centers.resize(numClusters);
   }

   /// Destructor
   ~KMeans() {
      // Make sure that the local states are cleaned up in case the query was
      // aborted early.
      for (auto* localState = consumeLocalStateList.load(); localState;) {
         unique_ptr<ConsumeLocalState> localStatePtr(localState);
         localState = localStatePtr->next;
      }
      for (auto* localState = localClusterCentersList.load(); localState;) {
         unique_ptr<LocalClustersEntry> localStatePtr(localState);
         localState = localStatePtr->next;
      }
   }

   /// Accept an input tuple
   void accept(udo::ExecutionState executionState, const InputTuple& input) {
      auto*& localState = reinterpret_cast<ConsumeLocalState*&>(getLocalState(executionState));
      if (!localState) {
         auto newLocalState = make_unique<ConsumeLocalState>(numClusters, udo::getRandom());
         newLocalState->next = consumeLocalStateList.load();
         while (!consumeLocalStateList.compare_exchange_weak(newLocalState->next, newLocalState.get()))
            ;

         auto tuplesRef = tuples.createLocalStorage(getThreadId(executionState));
         newLocalState->tuplesRef = tuplesRef;

         localState = newLocalState.get();
         // This will be deallocated in PrepareInitializeClusters
         newLocalState.release();
      }

      OutputTuple tuple;
      tuple.x = input.x;
      tuple.y = input.y;
      tuple.payload = input.payload;
      tuple.clusterId = 0;
      auto& insertedTuple = localState->tuplesRef->emplace_back(tuple);

      if (auto numTuples = localState->tuplesRef->size(); numTuples <= numClusters)
         localState->sample.getSample()[numTuples - 1] = &insertedTuple;
      else if (auto slot = localState->sample.getRandomSlot(); slot < numClusters)
         localState->sample.getSample()[slot] = &insertedTuple;
   }

   private:
   /// Prepare the initialization of clusters after all input points were seen
   Operation prepareInitializeClusters() {
      if (!prepareMutex.test_and_set()) {
         numTuples = 0;

         // Merge samples of all workers
         ReservoirSample<OutputTuple*> mergedSample(numClusters, 0);
         for (auto* consumeLocalState = consumeLocalStateList.exchange(nullptr); consumeLocalState;) {
            unique_ptr<ConsumeLocalState> localStatePtr(consumeLocalState);

            auto localNumTuples = localStatePtr->tuplesRef->size();
            numTuples += localNumTuples;
            localStatePtr->sample.setElementsSeen(localNumTuples);
            localStatePtr->sample.mergeInto(mergedSample);

            consumeLocalState = consumeLocalState->next;
         }

         if (numTuples < numClusters) {
#ifndef WASMUDO
            udo::printDebug("less points than clusters, aborting\n");
#endif
            abort();
         }

         // Write the sampled points into the cluster centers
         auto sample = mergedSample.getSample();
         for (unsigned i = 0; i < numClusters; ++i) {
            centers[i].x = sample[i]->x;
            centers[i].y = sample[i]->y;
         }
      }
      return FinishInitializeClusters;
   }

   /// Determine the next operation after cluster centers were initialized
   Operation finishInitializeClusters() {
      prepareMutex.clear();
      return PrepareAssociatePoints;
   }

   /// Prepare the associate points operation
   Operation prepareAssociatePoints() {
      if (!prepareMutex.test_and_set()) {
         numChangedPoints.store(0);
         tuplesIter = tuples.parallelIter();
      }
      return AssociatePoints;
   }

   /// Associate the points to the cluster centers
   Operation associatePoints(udo::ExecutionState executionState) {
      auto tuples = tuplesIter.next(getThreadId(executionState));
      if (!tuples)
         return FinishAssociatePoints;

      size_t localNumChangedPoints = 0;
      for (auto& tuple : *tuples) {
         uint16_t bestClusterId = 0;
         double currentDistance = distance(tuple, centers[0]);
         for (uint16_t i = 1; i < numClusters; ++i) {
            double newDistance = distance(tuple, centers[i]);
            if (newDistance < currentDistance) {
               bestClusterId = i;
               currentDistance = newDistance;
            }
         }
         if (bestClusterId != tuple.clusterId) {
            tuple.clusterId = bestClusterId;
            ++localNumChangedPoints;
         }
      }
      numChangedPoints.fetch_add(localNumChangedPoints);
      return AssociatePoints;
   }

   /// Decide whether to continue or not after associating points
   Operation finishAssociatePoints() {
      prepareMutex.clear();
      //XXX if (numChangedPoints.load() <= numTuples / 1000) {
      if (numIterations == 10) {
         return PrepareWriteOutput;
      } else {
         return PrepareRecalculateMeans;
      }
   }

   /// Prepare the recalculate means operation
   Operation prepareRecalculateMeans() {
      if (!prepareMutex.test_and_set()) {
         tuplesIter = tuples.parallelIter();
         ++numIterations;
      }
      return RecalculateMeans;
   }

   /// Calculate the means of the clusters
   Operation recalculateMeans(udo::ExecutionState executionState) {
      auto*& localClusters = reinterpret_cast<LocalClustersEntry*&>(getLocalState(executionState));
      if (!localClusters) {
         auto newLocalClusters = make_unique<LocalClustersEntry>();
         newLocalClusters->centers.resize(numClusters);
         newLocalClusters->next = localClusterCentersList.load();
         while (!localClusterCentersList.compare_exchange_weak(newLocalClusters->next, newLocalClusters.get()))
            ;

         localClusters = newLocalClusters.get();
         // This will be deallocated in FinishRecalculateMeans
         newLocalClusters.release();
      }

      auto tuples = tuplesIter.next(getThreadId(executionState));
      if (!tuples)
         return FinishRecalculateMeans;

      for (auto& tuple : *tuples) {
         auto& cluster = localClusters->centers[tuple.clusterId];
         ++cluster.numPoints;
         cluster.x += tuple.x;
         cluster.y += tuple.y;
      }

      return RecalculateMeans;
   }

   /// Switch to associate points after recalculating means
   Operation finishRecalculateMeans() {
      auto* localEntry = localClusterCentersList.exchange(nullptr);
      if (!localEntry)
         return PrepareAssociatePoints;

      prepareMutex.clear();

      // Loop over the local cluster centers and sum them up
      vector<LocalClusterCenter> mergedClusters(numClusters);
      while (localEntry) {
         unique_ptr<LocalClustersEntry> entryPtr(localEntry);
         for (unsigned i = 0; i < numClusters; ++i) {
            auto& mergedCenter = mergedClusters[i];
            auto& localCenter = entryPtr->centers[i];
            mergedCenter.numPoints += localCenter.numPoints;
            mergedCenter.x += localCenter.x;
            mergedCenter.y += localCenter.y;
         }
         localEntry = entryPtr->next;
      }

      // Write out the new cluster centers
      for (unsigned i = 0; i < numClusters; ++i) {
         auto& mergedCenter = mergedClusters[i];
         centers[i].x = mergedCenter.x / mergedCenter.numPoints;
         centers[i].y = mergedCenter.y / mergedCenter.numPoints;
      }

      return PrepareAssociatePoints;
   }

   /// Prepare to output the tuples
   Operation prepareWriteOutput() {
      if (!prepareMutex.test_and_set()) {
         ++numIterations;
         tuplesIter = tuples.parallelIter();
      }
      return WriteOutput;
   }

   public:
   /// Do extra work
   uint32_t extraWork(udo::ExecutionState executionState, uint32_t step) {
      switch (static_cast<Operation>(step)) {
         case PrepareInitializeClusters:
            return static_cast<uint32_t>(prepareInitializeClusters());
         case FinishInitializeClusters:
            return static_cast<uint32_t>(finishInitializeClusters());
         case PrepareAssociatePoints:
            return static_cast<uint32_t>(prepareAssociatePoints());
         case AssociatePoints:
            return static_cast<uint32_t>(associatePoints(executionState));
         case FinishAssociatePoints:
            return static_cast<uint32_t>(finishAssociatePoints());
         case PrepareRecalculateMeans:
            return static_cast<uint32_t>(prepareRecalculateMeans());
         case RecalculateMeans:
            return static_cast<uint32_t>(recalculateMeans(executionState));
         case FinishRecalculateMeans:
            return static_cast<uint32_t>(finishRecalculateMeans());
         case PrepareWriteOutput:
            return static_cast<uint32_t>(prepareWriteOutput());
         case WriteOutput:
            return static_cast<uint32_t>(WriteOutput);
      }
      __builtin_unreachable();
   }

   /// Produce the output
   bool process(udo::ExecutionState executionState) {
      auto tuples = tuplesIter.next(getThreadId(executionState));
      if (tuples) {
         for (auto& tuple : *tuples)
            emit<KMeans>(executionState, tuple);
         return false;
      } else {
         return true;
      }
   }
};
//---------------------------------------------------------------------------
template <size_t I>
decltype(auto) get(KMeans::InputTuple& tuple) {
   return tuple.get<I>();
}
//---------------------------------------------------------------------------
namespace std {
//---------------------------------------------------------------------------
template <>
struct tuple_size<KMeans::InputTuple> : std::integral_constant<std::size_t, 3> {};
//---------------------------------------------------------------------------
template <std::size_t I>
struct tuple_element<I, KMeans::InputTuple> {
};
//---------------------------------------------------------------------------
template <>
struct tuple_element<0, KMeans::InputTuple> {
   using type = double;
};
template <>
struct tuple_element<1, KMeans::InputTuple> {
   using type = double;
};
template <>
struct tuple_element<2, KMeans::InputTuple> {
   using type = uint64_t;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
#ifdef WASMUDO
// plugin-wasmudo -generate-cxx-header -extra-work KMeans 128 8 '' f64,f64,i64 'x f64,y f64,payload i64,clusterId i32' > wasmudo_udo_kmeans.hpp
#include "wasmudo_udo_kmeans.hpp"
#endif
//---------------------------------------------------------------------------
#ifdef UDO_STANDALONE
//---------------------------------------------------------------------------
int main(int argc, const char** argv) {
   bool argError = false;
   bool fullOutput = false;
   bool benchmark = false;
   string_view inputFileName;

   const char** argIt = argv;
   ++argIt;
   const char** argEnd = argv + argc;
   for (; argIt != argEnd; ++argIt) {
      string_view arg(*argIt);
      if (arg.empty())
         continue;
      if (arg == "--full-output") {
         fullOutput = true;
      } else if (arg == "--benchmark") {
         benchmark = true;
      } else {
         if (inputFileName.empty()) {
            inputFileName = arg;
         } else {
            argError = true;
            break;
         }
      }
   }

   if (!argError && inputFileName.empty())
      argError = true;

   if (argError) {
      cerr << "Usage: " << argv[0] << " [--full-output] [--benchmark] <input file>" << std::endl;
      return 2;
   }

   auto startParse = chrono::steady_clock::now();
   auto input = udo_util::parseCsv<KMeans::InputTuple>(inputFileName);
   auto endParse = chrono::steady_clock::now();

   auto numThreads = udo_util::getNumThreads();

   if (benchmark) {
      // We immediately discard the input that was just parsed above. We do
      // this so that the operating system has the chance to cache the input
      // file before we start the measurements.
      input.clear();

      for (unsigned i = 0; i < 3; ++i) {
         auto startParse = chrono::steady_clock::now();
         auto input = udo_util::parseCsv<KMeans::InputTuple>(inputFileName);
         auto endParse = chrono::steady_clock::now();

         auto parseNs = chrono::duration_cast<chrono::nanoseconds>(endParse - startParse).count();
         cout << "parse:" << parseNs << '\n';

         for (unsigned j = 0; j < 6; ++j) {
            udo::UDOStandalone<KMeans> standalone(numThreads, 10000);
            KMeans kMeans;

            auto start = chrono::steady_clock::now();
            auto output = standalone.run(kMeans, input);
            auto end = chrono::steady_clock::now();
            auto durationNs = chrono::duration_cast<chrono::nanoseconds>(end - start).count();
            // Don't measure the first run
            if (j > 0)
               cout << "exec:" << durationNs << '\n';
         }
      }
   } else {
      auto durationMs = chrono::duration_cast<chrono::milliseconds>(endParse - startParse).count();
      auto numTuples = input.size();
      cout << "Parsing: " << durationMs << " ms, " << numTuples << " tuples" << endl;

      udo::UDOStandalone<KMeans> standalone(numThreads, 10000);
      KMeans kMeans;
      auto output = standalone.run(kMeans, input);

      if (fullOutput) {
         for (auto& tuple : output)
            cout << tuple.x << ',' << tuple.y << ',' << tuple.payload << ',' << tuple.clusterId << '\n';
      } else {
         vector<size_t> clusterCounts(8);
         for (auto& tuple : output)
            ++clusterCounts[tuple.clusterId];

         for (size_t i = 0; i < clusterCounts.size(); ++i)
            cout << i << ": " << clusterCounts[i] << '\n';
      }
   }

   return 0;
}
//---------------------------------------------------------------------------
#endif
//---------------------------------------------------------------------------
