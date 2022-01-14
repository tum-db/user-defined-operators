#include <charconv>
#include <string_view>
//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
struct InputTuple {
   udo::String name;
   udo::String values;
};
//---------------------------------------------------------------------------
struct OutputTuple {
   udo::String name;
   int64_t value;
};
//---------------------------------------------------------------------------
class SplitArrays : public udo::UDOperator<InputTuple, OutputTuple> {
public:
   void consume(LocalState& /*localState*/, const InputTuple& input) {
      OutputTuple output;
      output.name = input.name;

      string_view values = input.values;

      const char* currentValueBegin = values.data();
      const char* it = values.data();
      const char* end = values.data() + values.size();
      for (; it != end; ++it) {
         if (*it == ',' || it + 1 == end) {
            auto currentValueEnd = *it == ',' ? it : end;
            if (currentValueBegin != currentValueEnd) {
               auto result = from_chars(currentValueBegin, currentValueEnd, output.value);
               if (result.ptr == currentValueEnd)
                  produceOutputTuple(output);
            }

            currentValueBegin = it + 1;
         }
      }
   }
};
