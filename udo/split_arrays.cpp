#include <charconv>
#include <string_view>
//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
class SplitArrays : public udo::UDOperator {
   public:
   struct InputTuple {
      udo::String name;
      udo::String values;
   };
   struct OutputTuple {
      udo::String name;
      int64_t value;
   };

   void accept(udo::ExecutionState executionState, const InputTuple& input) {
      OutputTuple output;
      output.name = static_cast<string_view>(input.name);

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
                  emit<SplitArrays>(executionState, output);
            }

            currentValueBegin = it + 1;
         }
      }
   }
};
//---------------------------------------------------------------------------
#ifdef WASMUDO
// plugin-wasmudo -generate-cxx-header -no-init -no-destroy -no-process SplitArrays 1 1 '' string,string 'name string,value i64' > wasmudo_split_arrays.hpp
#include "wasmudo_split_arrays.hpp"
#endif
//---------------------------------------------------------------------------
