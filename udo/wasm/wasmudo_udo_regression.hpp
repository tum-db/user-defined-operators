//---------------------------------------------------------------------------
/// Accept a tuple from the input
__attribute__((export_name("umbra_wasmudo_LinearRegression_accept"))) extern "C" void umbra_wasmudo_LinearRegression_accept(void* state, umbra_wasmudo_execution_state executionState, double arg0, double arg1)
{
   static_cast<LinearRegression*>(state)->accept(executionState, {arg0, arg1});
}
//---------------------------------------------------------------------------
/// Do additional processing after the entire input was seen
__attribute__((export_name("umbra_wasmudo_LinearRegression_process"))) extern "C" bool umbra_wasmudo_LinearRegression_process(void* state, umbra_wasmudo_execution_state executionState)
{
   return static_cast<LinearRegression*>(state)->process(executionState);
}
//---------------------------------------------------------------------------
/// Generate a tuple for the output
__attribute__((import_name("umbra_wasmudo_LinearRegression_emit"),noduplicate)) extern "C" void umbra_wasmudo_LinearRegression_emit(umbra_wasmudo_execution_state executionState, double arg0, double arg1, double arg2);
//---------------------------------------------------------------------------
namespace udo {
template <typename Derived>
requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& outputTuple) noexcept {
   static_assert(std::is_same_v<Derived, LinearRegression>);
   auto& [arg0, arg1, arg2] = outputTuple;
   umbra_wasmudo_LinearRegression_emit(executionState, arg0, arg1, arg2);
}
}
//---------------------------------------------------------------------------
/// Dummy main function, will not be called
int main() { return 42; }
//---------------------------------------------------------------------------
asm(
   ".section .custom_section.umbra_wasmudo_LinearRegression,\"\",@\n"
   ".int32 16\n"
   ".int32 8\n"
   ".int32 0\n"
   ".int32 2\n"
   ".ascii \"FF\"\n"
   ".int32 3\n"
   ".int32 1\n"
   ".ascii \"aF\"\n"
   ".int32 1\n"
   ".ascii \"bF\"\n"
   ".int32 1\n"
   ".ascii \"cF\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 37\n"
   ".ascii \"umbra_wasmudo_LinearRegression_accept\"\n"
   ".int32 0\n"
   ".int32 38\n"
   ".ascii \"umbra_wasmudo_LinearRegression_process\"\n"
   ".int32 35\n"
   ".ascii \"umbra_wasmudo_LinearRegression_emit\"\n"
);
//---------------------------------------------------------------------------
