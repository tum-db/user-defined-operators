#ifndef H_udo_runtime_UDOStandalone
#define H_udo_runtime_UDOStandalone
//---------------------------------------------------------------------------
#include "udo/UDOperator.hpp"
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <span>
#include <thread>
#include <vector>
//---------------------------------------------------------------------------
namespace udo {
//---------------------------------------------------------------------------
void printDebug(const char* msg, uint64_t size)
// Print a debug message
{
   std::cerr << std::string_view(msg, size);
}
//---------------------------------------------------------------------------
uint64_t getRandom()
// Get a random number
{
   auto rand = std::random_device();
   std::uniform_int_distribution<uint64_t> distr;
   return distr(rand);
}
//---------------------------------------------------------------------------
/// The common base class for the UDOStandalone class below
template <typename OT>
class UDOStandaloneBase {
   protected:
   /// The output for the running UDO
   static std::span<OT> standaloneOutput;
   /// The next valid index to write an output tuple
   static std::atomic<uint64_t> standaloneOutputIndex;

   public:
   static void produceOutputTuple(const OT& output) noexcept {
      uint64_t index = standaloneOutputIndex.fetch_add(1, std::memory_order_relaxed);
      if (index < standaloneOutput.size())
         standaloneOutput[index] = output;
   }
};
//---------------------------------------------------------------------------
// The output for the running UDO
template <typename OT>
std::span<OT> udo::UDOStandaloneBase<OT>::standaloneOutput = {};
//---------------------------------------------------------------------------
// The next valid index to write an output tuple
template <typename OT>
std::atomic<uint64_t> udo::UDOStandaloneBase<OT>::standaloneOutputIndex = {};
//---------------------------------------------------------------------------
/// The helper class to run an UDO standalone, i.e. without the database.
template <typename UDO>
class UDOStandalone : public UDOStandaloneBase<typename UDO::OutputTuple> {
   private:
   /// The possible states of the execution
   enum class ExecutionState : uint32_t {
      Input = 0,
      ExtraWork = 1,
      Output = 2,
      End = 3
   };

   /// The number of threads that should be used
   size_t numThreads;
   /// The morsel size for the input
   size_t morselSize;

   /// The input for the UDO
   std::span<const typename UDO::InputTuple> input;
   /// The current index for the input
   std::atomic<uint64_t> inputIndex;
   /// The last execution state (contains ExecutionState and the stepId of the UDO)
   uint64_t lastExecutionState;
   /// The number of threads waiting for the next execution state
   size_t numWaitingThreads;
   /// The mutex to synchronize execution states
   std::mutex executionMutex;
   /// The condition variable to synchronize execution states
   std::condition_variable executionCv;

   /// The main function for the threads
   void threadMain(UDO& udo) {
      typename UDO::LocalState localState;
      while (true) {
         auto executionState = static_cast<ExecutionState>(lastExecutionState >> 32);
         uint64_t nextExecutionState = lastExecutionState;

         switch (executionState) {
            case ExecutionState::Input: {
               std::memset(localState.data, 0, sizeof(localState.data));
               auto startIndex = inputIndex.fetch_add(morselSize);
               if (startIndex < input.size()) {
                  for (size_t i = 0; i < morselSize && startIndex + i < input.size(); ++i)
                     udo.consume(localState, input[startIndex + i]);
               } else {
                  nextExecutionState = static_cast<uint64_t>(ExecutionState::ExtraWork) << 32;
               }
               break;
            }

            case ExecutionState::ExtraWork: {
               std::memset(localState.data, 0, sizeof(localState.data));
               auto stepId = static_cast<uint32_t>(lastExecutionState);
               if (stepId != UDO::extraWorkDone)
                  stepId = udo.extraWork(localState, stepId);

               if (stepId == UDO::extraWorkDone) {
                  nextExecutionState = static_cast<uint64_t>(ExecutionState::Output) << 32;
               } else {
                  nextExecutionState = (static_cast<uint64_t>(ExecutionState::ExtraWork) << 32) | stepId;
               }
               break;
            }

            case ExecutionState::Output: {
               std::memset(localState.data, 0, sizeof(localState.data));
               while (!udo.postProduce(localState))
                  ;
               nextExecutionState = static_cast<uint64_t>(ExecutionState::End) << 32;
            }

            case ExecutionState::End:
               return;
         }

         if (nextExecutionState != lastExecutionState) {
            std::unique_lock lock(executionMutex);
            ++numWaitingThreads;
            if (numWaitingThreads == numThreads) {
               lastExecutionState = nextExecutionState;
               numWaitingThreads = 0;
               executionCv.notify_all();
            } else {
               auto localExecutionState = lastExecutionState;
               executionCv.wait(lock, [&] { return localExecutionState != lastExecutionState; });
            }
         }
      }
   };

   public:
   /// Constructor
   explicit UDOStandalone(size_t numThreads, size_t morselSize = 1000)
      : numThreads(numThreads), morselSize(morselSize) {}

   /// Get the output generated by the UDO
   static std::span<typename UDO::OutputTuple> getOutput() {
      auto size = UDOStandaloneBase<typename UDO::OutputTuple>::standaloneOutputIndex.load();
      auto totalOutput = UDOStandaloneBase<typename UDO::OutputTuple>::standaloneOutput;
      return totalOutput.subspan(0, size);
   }

   /// Run this UDO with the given input
   uint64_t run(UDO& udo, std::span<const typename UDO::InputTuple> input, std::span<typename UDO::OutputTuple> output) {
      this->input = input;
      inputIndex.store(0);
      lastExecutionState = 0;
      numWaitingThreads = 0;

      UDOStandaloneBase<typename UDO::OutputTuple>::standaloneOutput = output;
      auto& outputIndex = UDOStandaloneBase<typename UDO::OutputTuple>::standaloneOutputIndex;
      outputIndex.store(0);

      size_t realNumThreads = numThreads;
      if (realNumThreads == 0)
         realNumThreads = 1;

      std::vector<std::thread> threads;
      threads.reserve(realNumThreads);

      for (size_t i = 0; i < realNumThreads; ++i)
         threads.emplace_back([this, &udo] { threadMain(udo); });

      for (auto& t : threads)
         t.join();

      return outputIndex.load();
   }
};
//---------------------------------------------------------------------------
template <typename IT, typename OT>
void UDOperator<IT, OT>::produceOutputTuple(const OT& output) noexcept {
   UDOStandaloneBase<OT>::produceOutputTuple(output);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
#endif
