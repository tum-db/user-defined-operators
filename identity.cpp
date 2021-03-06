//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
struct Tuple {
   uint64_t a;
};
//---------------------------------------------------------------------------
class Identity : public udo::UDOperator<Tuple, Tuple> {
   public:
   void consume(LocalState& /*localState*/, const Tuple& input) {
      produceOutputTuple(input);
   }
};
//---------------------------------------------------------------------------
