#include <array>
#include <atomic>
#include <string_view>
//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
using namespace std::literals::string_view_literals;
//---------------------------------------------------------------------------
class CountLifestyle : public udo::UDOperator {
   atomic<uint64_t> lifestyle = 0;
   atomic<uint64_t> other = 0;
   atomic_flag outputMutex;

   public:
   struct InputTuple {
      udo::String word;
   };
   struct OutputTuple {
      udo::String word;
      uint64_t wordCount;
   };

   void accept(udo::ExecutionState /*executionState*/, const InputTuple& tuple) {
      if (tuple.word == "lifestyle"sv)
         lifestyle.fetch_add(1, memory_order_relaxed);
      else
         other.fetch_add(1, memory_order_relaxed);
   }

   bool process(udo::ExecutionState executionState) {
      if (outputMutex.test_and_set(memory_order_relaxed))
         return true;

      array<OutputTuple, 2> result = {{{"lifestyle"sv, lifestyle.load()}, {"other"sv, other.load()}}};

      for (auto& tuple : result)
         emit<CountLifestyle>(executionState, tuple);

      return true;
   }
};
//---------------------------------------------------------------------------
#ifdef WASMUDO
// plugin-wasmudo -generate-cxx-header -no-destroy CountLifestyle 24 8 '' string 'word string,wordCount i64' > wasmudo_count_lifestyle.hpp
#include "wasmudo_count_lifestyle.hpp"
#endif
//---------------------------------------------------------------------------
