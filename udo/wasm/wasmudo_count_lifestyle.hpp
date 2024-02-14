//---------------------------------------------------------------------------
/// Initialize the UDO
__attribute__((export_name("umbra_wasmudo_CountLifestyle_init"))) extern "C" void umbra_wasmudo_CountLifestyle_init(void* state)
{
   new (state) CountLifestyle();
}
//---------------------------------------------------------------------------
/// Accept a tuple from the input
__attribute__((export_name("umbra_wasmudo_CountLifestyle_accept"))) extern "C" void umbra_wasmudo_CountLifestyle_accept(void* state, umbra_wasmudo_execution_state executionState, umbra_wasmudo_string arg0)
{
   static_cast<CountLifestyle*>(state)->accept(executionState, {udo::String(arg0)});
}
//---------------------------------------------------------------------------
/// Do additional processing after the entire input was seen
__attribute__((export_name("umbra_wasmudo_CountLifestyle_process"))) extern "C" bool umbra_wasmudo_CountLifestyle_process(void* state, umbra_wasmudo_execution_state executionState)
{
   return static_cast<CountLifestyle*>(state)->process(executionState);
}
//---------------------------------------------------------------------------
/// Generate a tuple for the output
__attribute__((import_name("umbra_wasmudo_CountLifestyle_emit"),noduplicate)) extern "C" void umbra_wasmudo_CountLifestyle_emit(umbra_wasmudo_execution_state executionState, umbra_wasmudo_string arg0, uint64_t arg1);
//---------------------------------------------------------------------------
namespace udo {
template <typename Derived>
requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& outputTuple) noexcept {
   static_assert(std::is_same_v<Derived, CountLifestyle>);
   auto& [arg0, arg1] = outputTuple;
   umbra_wasmudo_CountLifestyle_emit(executionState, arg0.asRaw(), arg1);
}
}
//---------------------------------------------------------------------------
/// Dummy main function, will not be called
int main() { return 42; }
//---------------------------------------------------------------------------
asm(
   ".section .custom_section.umbra_wasmudo_CountLifestyle,\"\",@\n"
   ".int32 24\n"
   ".int32 8\n"
   ".int32 0\n"
   ".int32 1\n"
   ".ascii \"S\"\n"
   ".int32 2\n"
   ".int32 4\n"
   ".ascii \"wordS\"\n"
   ".int32 9\n"
   ".ascii \"wordCountI\"\n"
   ".int32 33\n"
   ".ascii \"umbra_wasmudo_CountLifestyle_init\"\n"
   ".int32 0\n"
   ".int32 35\n"
   ".ascii \"umbra_wasmudo_CountLifestyle_accept\"\n"
   ".int32 0\n"
   ".int32 36\n"
   ".ascii \"umbra_wasmudo_CountLifestyle_process\"\n"
   ".int32 33\n"
   ".ascii \"umbra_wasmudo_CountLifestyle_emit\"\n"
);
//---------------------------------------------------------------------------
