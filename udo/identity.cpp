//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
class Identity : public udo::UDOperator {
   public:
   struct InputTuple {
      uint64_t a;
   };

   using OutputTuple = InputTuple;

   void accept(udo::ExecutionState executionState, const InputTuple& input) {
      emit<Identity>(executionState, input);
   }
};
//---------------------------------------------------------------------------
#ifdef WASMUDO
// plugin-wasmudo -generate-cxx-header -no-init -no-destroy -no-process Identity 1 1 '' i64 'a i64' > wasmudo_identity.hpp
#include "wasmudo_identity.hpp"
#endif
//---------------------------------------------------------------------------
