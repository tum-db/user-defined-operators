#ifndef H_udo_runtime_UDOStandalone
#define H_udo_runtime_UDOStandalone
//---------------------------------------------------------------------------
#include "udo/ChunkedStorage.hpp"
#include "udo/UDOperator.hpp"
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <mutex>
#include <new>
#include <random>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
//---------------------------------------------------------------------------
namespace udo {
//---------------------------------------------------------------------------
inline void printDebug(const char* msg, uint64_t size)
// Print a debug message
{
   std::cerr << std::string_view(msg, size);
}
//---------------------------------------------------------------------------
inline uint64_t getRandom()
// Get a random number
{
   auto rand = std::random_device();
   std::uniform_int_distribution<uint64_t> distr;
   return distr(rand);
}
//---------------------------------------------------------------------------
inline uint32_t ExecutionState::getThreadId()
// Get the thread id of the current thread
{
   return *reinterpret_cast<uint32_t*>(&getLocalState() + 1);
}
//---------------------------------------------------------------------------
inline LocalState& ExecutionState::getLocalState()
// Get the local state for the thread of this execution state
{
   return *static_cast<LocalState*>(data[0]);
}
//---------------------------------------------------------------------------
/// The emit function used by a UDO in the standalone execution
template <typename UDO>
static void standaloneEmit(ExecutionState executionState, const typename UDO::OutputTuple& output) noexcept {
   void* data[2];
   std::memcpy(&data, &executionState, sizeof(executionState));
   auto* outputStorage = static_cast<ChunkedStorage<typename UDO::OutputTuple>*>(data[1]);
   outputStorage->push_back(output);
}
//---------------------------------------------------------------------------
template <typename Derived>
   requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& output) noexcept {
   standaloneEmit<Derived>(executionState, output);
}
//---------------------------------------------------------------------------
/// The helper class to run an UDO standalone, i.e. without the database.
template <typename UDO>
class UDOStandalone {
   private:
   /// The execution state used by the standalone execution
   struct StandaloneExecutionState {
      /// The pointer to the local state
      LocalState* localState;
      /// The pointer to the storage for the output
      ChunkedStorage<typename UDO::OutputTuple>* outputStorage;
   };

   /// The possible states of the execution
   enum class State : uint32_t {
      Input = 0,
      ExtraWork = 1,
      Process = 2,
      End = 3
   };

   /// The state of a thread
   struct ThreadState {
      /// The output collected by this thread
      ChunkedStorage<typename UDO::OutputTuple> output;
      /// The state of the next thread
      ThreadState* next;
   };

   /// The number of threads that should be used
   size_t numThreads;
   /// The morsel size for the input
   size_t morselSize;

   /// The iterator for the input of the UDO
   typename ParallelChunkedStorage<typename UDO::InputTuple>::const_parallel_iterator inputIterator;
   /// The head of the thread list
   std::atomic<ThreadState*> threadList = nullptr;
   /// The last state info, contains the State value and the stepId of the UDO
   uint64_t lastStateInfo;
   /// The number of threads waiting for the next execution state
   size_t numWaitingThreads;
   /// The mutex to synchronize execution states
   std::mutex executionMutex;
   /// The condition variable to synchronize execution states
   std::condition_variable executionCv;

   /// The main function for the threads
   void threadMain(UDO& udo, uint32_t threadId) {
      auto* threadState = new ThreadState;
      threadState->next = threadList.load();
      while (!threadList.compare_exchange_weak(threadState->next, threadState))
         ;

      alignas(LocalState) std::byte localStateStorage[sizeof(LocalState) * 2];
      auto* localState = reinterpret_cast<LocalState*>(localStateStorage);
      *reinterpret_cast<uint32_t*>(localState + 1) = threadId;

      auto clearLocalState = [&] {
         std::memset(localStateStorage, 0, sizeof(LocalState));
      };
      clearLocalState();

      StandaloneExecutionState standaloneExecState;
      standaloneExecState.localState = localState;
      standaloneExecState.outputStorage = &threadState->output;
      alignas(ExecutionState) std::byte execStateData[sizeof(ExecutionState)];
      std::memcpy(execStateData, &standaloneExecState, sizeof(standaloneExecState));
      auto execState = *reinterpret_cast<ExecutionState*>(execStateData);

      static constexpr bool hasExtraWork = requires(UDO & udo, ExecutionState executionState, uint32_t stepId) {
                                              udo.extraWork(execState, stepId);
                                           };

      while (true) {
         auto state = static_cast<State>(lastStateInfo >> 32);
         uint64_t nextStateInfo = lastStateInfo;

         switch (state) {
            case State::Input: {
               auto range = inputIterator.next(threadId);
               if (range) {
                  for (auto& tuple : *range)
                     udo.accept(execState, tuple);
               } else {
                  if constexpr (hasExtraWork)
                     nextStateInfo = static_cast<uint64_t>(State::ExtraWork) << 32;
                  else
                     nextStateInfo = static_cast<uint64_t>(State::Process) << 32;
               }
               break;
            }

            case State::ExtraWork: {
               if constexpr (hasExtraWork) {
                  auto stepId = static_cast<uint32_t>(lastStateInfo);
                  if (stepId != UDO::extraWorkDone)
                     stepId = udo.extraWork(execState, stepId);

                  if (stepId == UDO::extraWorkDone) {
                     nextStateInfo = static_cast<uint64_t>(State::Process) << 32;
                  } else {
                     nextStateInfo = (static_cast<uint64_t>(State::ExtraWork) << 32) | stepId;
                  }
               } else {
                  nextStateInfo = static_cast<uint64_t>(State::Process) << 32;
               }
               break;
            }

            case State::Process: {
               while (!udo.process(execState))
                  ;
               nextStateInfo = static_cast<uint64_t>(State::End) << 32;
            }

            case State::End:
               return;
         }

         if (nextStateInfo != lastStateInfo) {
            clearLocalState();

            std::unique_lock lock(executionMutex);
            ++numWaitingThreads;
            if (numWaitingThreads == numThreads) {
               // Only the last thread executing the current state gets here.
               // So, update the state info and notify all other threads.
               lastStateInfo = nextStateInfo;
               numWaitingThreads = 0;
               executionCv.notify_all();
            } else {
               auto localExecutionState = lastStateInfo;
               executionCv.wait(lock, [&] { return localExecutionState != lastStateInfo; });
            }
         }
      }
   };

   public:
   /// Constructor
   explicit UDOStandalone(size_t numThreads, size_t morselSize = 1000)
      : numThreads(numThreads), morselSize(morselSize) {}

   /// Run this UDO with the given input and return the output
   ChunkedStorage<typename UDO::OutputTuple> run(UDO& udo, const ParallelChunkedStorage<typename UDO::InputTuple>& input) {
      inputIterator = input.parallelIter();
      lastStateInfo = 0;
      numWaitingThreads = 0;

      size_t realNumThreads = numThreads;
      if (realNumThreads == 0)
         realNumThreads = 1;

      std::vector<std::thread> threads;
      threads.reserve(realNumThreads);

      for (size_t i = 0; i < realNumThreads; ++i)
         threads.emplace_back([this, i, &udo] { threadMain(udo, i); });

      for (auto& t : threads)
         t.join();

      ChunkedStorage<typename UDO::OutputTuple> output;

      for (auto* threadState = threadList.load(); threadState;) {
         output.merge(std::move(threadState->output));
         auto* next = threadState->next;
         delete threadState;
         threadState = next;
      }

      return output;
   }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
#endif
