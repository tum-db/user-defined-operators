//---------------------------------------------------------------------------
/// Initialize the UDO
__attribute__((export_name("umbra_wasmudo_CreateWords_init"))) extern "C" void umbra_wasmudo_CreateWords_init(void* state, uint64_t arg0)
{
   new (state) CreateWords(arg0);
}
//---------------------------------------------------------------------------
/// Do additional processing after the entire input was seen
__attribute__((export_name("umbra_wasmudo_CreateWords_process"))) extern "C" bool umbra_wasmudo_CreateWords_process(void* state, umbra_wasmudo_execution_state executionState)
{
   return static_cast<CreateWords*>(state)->process(executionState);
}
//---------------------------------------------------------------------------
/// Generate a tuple for the output
__attribute__((import_name("umbra_wasmudo_CreateWords_emit"),noduplicate)) extern "C" void umbra_wasmudo_CreateWords_emit(umbra_wasmudo_execution_state executionState, umbra_wasmudo_string arg0);
//---------------------------------------------------------------------------
namespace udo {
template <typename Derived>
requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& outputTuple) noexcept {
   static_assert(std::is_same_v<Derived, CreateWords>);
   auto& [arg0] = outputTuple;
   umbra_wasmudo_CreateWords_emit(executionState, arg0.asRaw());
}
}
//---------------------------------------------------------------------------
/// Dummy main function, will not be called
int main() { return 42; }
//---------------------------------------------------------------------------
asm(
   ".section .custom_section.umbra_wasmudo_CreateWords,\"\",@\n"
   ".int32 16\n"
   ".int32 8\n"
   ".int32 1\n"
   ".ascii \"I\"\n"
   ".int32 0\n"
   ".int32 1\n"
   ".int32 4\n"
   ".ascii \"wordS\"\n"
   ".int32 30\n"
   ".ascii \"umbra_wasmudo_CreateWords_init\"\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 0\n"
   ".int32 33\n"
   ".ascii \"umbra_wasmudo_CreateWords_process\"\n"
   ".int32 30\n"
   ".ascii \"umbra_wasmudo_CreateWords_emit\"\n"
);
//---------------------------------------------------------------------------
