//---------------------------------------------------------------------------
/// Accept a tuple from the input
__attribute__((export_name("umbra_wasmudo_ContainsDatabase_accept"))) extern "C" void umbra_wasmudo_ContainsDatabase_accept(void* state, umbra_wasmudo_execution_state executionState, umbra_wasmudo_string arg0)
{
   static_cast<ContainsDatabase*>(state)->accept(executionState, {udo::String(arg0)});
}
//---------------------------------------------------------------------------
/// Generate a tuple for the output
__attribute__((import_name("umbra_wasmudo_ContainsDatabase_emit"),noduplicate)) extern "C" void umbra_wasmudo_ContainsDatabase_emit(umbra_wasmudo_execution_state executionState, umbra_wasmudo_string arg0);
//---------------------------------------------------------------------------
namespace udo {
template <typename Derived>
requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& outputTuple) noexcept {
   static_assert(std::is_same_v<Derived, ContainsDatabase>);
   auto& [arg0] = outputTuple;
   umbra_wasmudo_ContainsDatabase_emit(executionState, arg0.asRaw());
}
}
//---------------------------------------------------------------------------
/// Dummy main function, will not be called
int main() { return 42; }
//---------------------------------------------------------------------------
asm(
   ".section .custom_section.umbra_wasmudo_ContainsDatabase,\"\",@\n"
   ".int32 1\n"
   ".int32 1\n"
   ".int32 0\n"
   ".int32 1\n"
   ".ascii \"S\"\n"
   ".int32 1\n"
   ".int32 4\n"
   ".ascii \"wordS\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 37\n"
   ".ascii \"umbra_wasmudo_ContainsDatabase_accept\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 35\n"
   ".ascii \"umbra_wasmudo_ContainsDatabase_emit\"\n"
);
//---------------------------------------------------------------------------
