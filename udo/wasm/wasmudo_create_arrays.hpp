//---------------------------------------------------------------------------
/// Initialize the UDO
__attribute__((export_name("umbra_wasmudo_CreateArrays_init"))) extern "C" void umbra_wasmudo_CreateArrays_init(void* state, uint64_t arg0)
{
   new (state) CreateArrays(arg0);
}
//---------------------------------------------------------------------------
/// Do additional processing after the entire input was seen
__attribute__((export_name("umbra_wasmudo_CreateArrays_process"))) extern "C" bool umbra_wasmudo_CreateArrays_process(void* state, umbra_wasmudo_execution_state executionState)
{
   return static_cast<CreateArrays*>(state)->process(executionState);
}
//---------------------------------------------------------------------------
/// Generate a tuple for the output
__attribute__((import_name("umbra_wasmudo_CreateArrays_emit"),noduplicate)) extern "C" void umbra_wasmudo_CreateArrays_emit(umbra_wasmudo_execution_state executionState, umbra_wasmudo_string arg0, umbra_wasmudo_string arg1);
//---------------------------------------------------------------------------
namespace udo {
template <typename Derived>
requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& outputTuple) noexcept {
   static_assert(std::is_same_v<Derived, CreateArrays>);
   auto& [arg0, arg1] = outputTuple;
   umbra_wasmudo_CreateArrays_emit(executionState, arg0.asRaw(), arg1.asRaw());
}
}
//---------------------------------------------------------------------------
/// Dummy main function, will not be called
int main() { return 42; }
//---------------------------------------------------------------------------
asm(
   ".section .custom_section.umbra_wasmudo_CreateArrays,\"\",@\n"
   ".int32 16\n"
   ".int32 8\n"
   ".int32 1\n"
   ".ascii \"I\"\n"
   ".int32 0\n"
   ".int32 2\n"
   ".int32 4\n"
   ".ascii \"nameS\"\n"
   ".int32 6\n"
   ".ascii \"valuesS\"\n"
   ".int32 31\n"
   ".ascii \"umbra_wasmudo_CreateArrays_init\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 34\n"
   ".ascii \"umbra_wasmudo_CreateArrays_process\"\n"
   ".int32 31\n"
   ".ascii \"umbra_wasmudo_CreateArrays_emit\"\n"
);
//---------------------------------------------------------------------------
