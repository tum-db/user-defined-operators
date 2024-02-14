//---------------------------------------------------------------------------
/// Accept a tuple from the input
__attribute__((export_name("umbra_wasmudo_Identity_accept"))) extern "C" void umbra_wasmudo_Identity_accept(void* state, umbra_wasmudo_execution_state executionState, uint64_t arg0)
{
   static_cast<Identity*>(state)->accept(executionState, {arg0});
}
//---------------------------------------------------------------------------
/// Generate a tuple for the output
__attribute__((import_name("umbra_wasmudo_Identity_emit"),noduplicate)) extern "C" void umbra_wasmudo_Identity_emit(umbra_wasmudo_execution_state executionState, uint64_t arg0);
//---------------------------------------------------------------------------
namespace udo {
template <typename Derived>
requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& outputTuple) noexcept {
   static_assert(std::is_same_v<Derived, Identity>);
   auto& [arg0] = outputTuple;
   umbra_wasmudo_Identity_emit(executionState, arg0);
}
}
//---------------------------------------------------------------------------
/// Dummy main function, will not be called
int main() { return 42; }
//---------------------------------------------------------------------------
asm(
   ".section .custom_section.umbra_wasmudo_Identity,\"\",@\n"
   ".int32 1\n"
   ".int32 1\n"
   ".int32 0\n"
   ".int32 1\n"
   ".ascii \"I\"\n"
   ".int32 1\n"
   ".int32 1\n"
   ".ascii \"aI\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 29\n"
   ".ascii \"umbra_wasmudo_Identity_accept\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 27\n"
   ".ascii \"umbra_wasmudo_Identity_emit\"\n"
);
//---------------------------------------------------------------------------
