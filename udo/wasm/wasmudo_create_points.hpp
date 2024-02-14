//---------------------------------------------------------------------------
/// Initialize the UDO
__attribute__((export_name("umbra_wasmudo_CreatePoints_init"))) extern "C" void umbra_wasmudo_CreatePoints_init(void* state, uint64_t arg0)
{
   new (state) CreatePoints(arg0);
}
//---------------------------------------------------------------------------
/// Do additional processing after the entire input was seen
__attribute__((export_name("umbra_wasmudo_CreatePoints_process"))) extern "C" bool umbra_wasmudo_CreatePoints_process(void* state, umbra_wasmudo_execution_state executionState)
{
   return static_cast<CreatePoints*>(state)->process(executionState);
}
//---------------------------------------------------------------------------
/// Generate a tuple for the output
__attribute__((import_name("umbra_wasmudo_CreatePoints_emit"),noduplicate)) extern "C" void umbra_wasmudo_CreatePoints_emit(umbra_wasmudo_execution_state executionState, double arg0, double arg1, uint32_t arg2);
//---------------------------------------------------------------------------
namespace udo {
template <typename Derived>
requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& outputTuple) noexcept {
   static_assert(std::is_same_v<Derived, CreatePoints>);
   auto& [arg0, arg1, arg2] = outputTuple;
   umbra_wasmudo_CreatePoints_emit(executionState, arg0, arg1, arg2);
}
}
//---------------------------------------------------------------------------
/// Dummy main function, will not be called
int main() { return 42; }
//---------------------------------------------------------------------------
asm(
   ".section .custom_section.umbra_wasmudo_CreatePoints,\"\",@\n"
   ".int32 16\n"
   ".int32 8\n"
   ".int32 1\n"
   ".ascii \"I\"\n"
   ".int32 0\n"
   ".int32 3\n"
   ".int32 1\n"
   ".ascii \"xF\"\n"
   ".int32 1\n"
   ".ascii \"yF\"\n"
   ".int32 9\n"
   ".ascii \"clusterIdi\"\n"
   ".int32 31\n"
   ".ascii \"umbra_wasmudo_CreatePoints_init\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 34\n"
   ".ascii \"umbra_wasmudo_CreatePoints_process\"\n"
   ".int32 31\n"
   ".ascii \"umbra_wasmudo_CreatePoints_emit\"\n"
);
//---------------------------------------------------------------------------
