//---------------------------------------------------------------------------
/// Accept a tuple from the input
__attribute__((export_name("umbra_wasmudo_SplitArrays_accept"))) extern "C" void umbra_wasmudo_SplitArrays_accept(void* state, umbra_wasmudo_execution_state executionState, umbra_wasmudo_string arg0, umbra_wasmudo_string arg1)
{
   static_cast<SplitArrays*>(state)->accept(executionState, {udo::String(arg0), udo::String(arg1)});
}
//---------------------------------------------------------------------------
/// Generate a tuple for the output
__attribute__((import_name("umbra_wasmudo_SplitArrays_emit"),noduplicate)) extern "C" void umbra_wasmudo_SplitArrays_emit(umbra_wasmudo_execution_state executionState, umbra_wasmudo_string arg0, uint64_t arg1);
//---------------------------------------------------------------------------
namespace udo {
template <typename Derived>
requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& outputTuple) noexcept {
   static_assert(std::is_same_v<Derived, SplitArrays>);
   auto& [arg0, arg1] = outputTuple;
   umbra_wasmudo_SplitArrays_emit(executionState, arg0.asRaw(), arg1);
}
}
//---------------------------------------------------------------------------
/// Dummy main function, will not be called
int main() { return 42; }
//---------------------------------------------------------------------------
asm(
   ".section .custom_section.umbra_wasmudo_SplitArrays,\"\",@\n"
   ".int32 1\n"
   ".int32 1\n"
   ".int32 0\n"
   ".int32 2\n"
   ".ascii \"SS\"\n"
   ".int32 2\n"
   ".int32 4\n"
   ".ascii \"nameS\"\n"
   ".int32 5\n"
   ".ascii \"valueI\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 32\n"
   ".ascii \"umbra_wasmudo_SplitArrays_accept\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 30\n"
   ".ascii \"umbra_wasmudo_SplitArrays_emit\"\n"
);
//---------------------------------------------------------------------------
