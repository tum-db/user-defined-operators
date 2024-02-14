//---------------------------------------------------------------------------
/// Initialize the UDO
__attribute__((export_name("umbra_wasmudo_CreateRegressionPoints_init"))) extern "C" void umbra_wasmudo_CreateRegressionPoints_init(void* state, double arg0, double arg1, double arg2, uint64_t arg3)
{
   new (state) CreateRegressionPoints(arg0, arg1, arg2, arg3);
}
//---------------------------------------------------------------------------
/// Do additional processing after the entire input was seen
__attribute__((export_name("umbra_wasmudo_CreateRegressionPoints_process"))) extern "C" bool umbra_wasmudo_CreateRegressionPoints_process(void* state, umbra_wasmudo_execution_state executionState)
{
   return static_cast<CreateRegressionPoints*>(state)->process(executionState);
}
//---------------------------------------------------------------------------
/// Generate a tuple for the output
__attribute__((import_name("umbra_wasmudo_CreateRegressionPoints_emit"),noduplicate)) extern "C" void umbra_wasmudo_CreateRegressionPoints_emit(umbra_wasmudo_execution_state executionState, double arg0, double arg1);
//---------------------------------------------------------------------------
namespace udo {
template <typename Derived>
requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& outputTuple) noexcept {
   static_assert(std::is_same_v<Derived, CreateRegressionPoints>);
   auto& [arg0, arg1] = outputTuple;
   umbra_wasmudo_CreateRegressionPoints_emit(executionState, arg0, arg1);
}
}
//---------------------------------------------------------------------------
/// Dummy main function, will not be called
int main() { return 42; }
//---------------------------------------------------------------------------
asm(
   ".section .custom_section.umbra_wasmudo_CreateRegressionPoints,\"\",@\n"
   ".int32 40\n"
   ".int32 8\n"
   ".int32 4\n"
   ".ascii \"FFFI\"\n"
   ".int32 0\n"
   ".int32 2\n"
   ".int32 1\n"
   ".ascii \"xF\"\n"
   ".int32 1\n"
   ".ascii \"yF\"\n"
   ".int32 41\n"
   ".ascii \"umbra_wasmudo_CreateRegressionPoints_init\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 44\n"
   ".ascii \"umbra_wasmudo_CreateRegressionPoints_process\"\n"
   ".int32 41\n"
   ".ascii \"umbra_wasmudo_CreateRegressionPoints_emit\"\n"
);
//---------------------------------------------------------------------------
