#include <array>
#include <atomic>
#include <string_view>
//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
using namespace std::literals::string_view_literals;
//---------------------------------------------------------------------------
struct InputTuple {
   udo::String word;
};
//---------------------------------------------------------------------------
struct OutputTuple {
   udo::String word;
   uint64_t wordCount;
};
//---------------------------------------------------------------------------
class CountLifestyle : public udo::UDOperator<InputTuple, OutputTuple> {
   atomic<uint64_t> lifestyle = 0;
   atomic<uint64_t> other = 0;
   atomic_flag outputMutex;

   public:
   void consume(LocalState& /*localState*/, const InputTuple& tuple) {
      if (tuple.word == "lifestyle"sv)
         lifestyle.fetch_add(1, memory_order_relaxed);
      else
         other.fetch_add(1, memory_order_relaxed);
   }

   bool postProduce(LocalState& /*localState*/) {
      if (outputMutex.test_and_set(memory_order_relaxed))
         return true;

      array<OutputTuple, 2> result = {{{"lifestyle"sv, lifestyle.load()}, {"other"sv, other.load()}}};

      for (auto& tuple : result)
         produceOutputTuple(tuple);

      return true;
   }
};
//---------------------------------------------------------------------------
