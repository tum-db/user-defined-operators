//---------------------------------------------------------------------------
/// Initialize the UDO
__attribute__((export_name("umbra_wasmudo_KMeans_init"))) extern "C" void umbra_wasmudo_KMeans_init(void* state)
{
   new (state) KMeans();
}
//---------------------------------------------------------------------------
/// Destroy the UDO
__attribute__((export_name("umbra_wasmudo_KMeans_destroy"))) extern "C" void umbra_wasmudo_KMeans_destroy(void* state)
{
   static_cast<KMeans*>(state)->~KMeans();
}
//---------------------------------------------------------------------------
/// Accept a tuple from the input
__attribute__((export_name("umbra_wasmudo_KMeans_accept"))) extern "C" void umbra_wasmudo_KMeans_accept(void* state, umbra_wasmudo_execution_state executionState, double arg0, double arg1, uint64_t arg2)
{
   static_cast<KMeans*>(state)->accept(executionState, {arg0, arg1, arg2});
}
//---------------------------------------------------------------------------
/// Do additional processing after the entire input was seen but before generating the output
__attribute__((export_name("umbra_wasmudo_KMeans_extra_work"))) extern "C" uint32_t umbra_wasmudo_KMeans_extra_work(void* state, umbra_wasmudo_execution_state executionState, uint32_t step)
{
   return static_cast<KMeans*>(state)->extraWork(executionState, step);
}
//---------------------------------------------------------------------------
/// Do additional processing after the entire input was seen
__attribute__((export_name("umbra_wasmudo_KMeans_process"))) extern "C" bool umbra_wasmudo_KMeans_process(void* state, umbra_wasmudo_execution_state executionState)
{
   return static_cast<KMeans*>(state)->process(executionState);
}
//---------------------------------------------------------------------------
/// Generate a tuple for the output
__attribute__((import_name("umbra_wasmudo_KMeans_emit"),noduplicate)) extern "C" void umbra_wasmudo_KMeans_emit(umbra_wasmudo_execution_state executionState, double arg0, double arg1, uint64_t arg2, uint32_t arg3);
//---------------------------------------------------------------------------
namespace udo {
template <typename Derived>
requires std::is_base_of_v<UDOperator, Derived>
void UDOperator::emit(ExecutionState executionState, const typename Derived::OutputTuple& outputTuple) noexcept {
   static_assert(std::is_same_v<Derived, KMeans>);
   auto& [arg0, arg1, arg2, arg3] = outputTuple;
   umbra_wasmudo_KMeans_emit(executionState, arg0, arg1, arg2, arg3);
}
}
//---------------------------------------------------------------------------
/// Dummy main function, will not be called
int main() { return 42; }
//---------------------------------------------------------------------------
asm(
   ".section .custom_section.umbra_wasmudo_KMeans,\"\",@\n"
   ".int32 128\n"
   ".int32 8\n"
   ".int32 0\n"
   ".int32 3\n"
   ".ascii \"FFI\"\n"
   ".int32 4\n"
   ".int32 1\n"
   ".ascii \"xF\"\n"
   ".int32 1\n"
   ".ascii \"yF\"\n"
   ".int32 7\n"
   ".ascii \"payloadI\"\n"
   ".int32 9\n"
   ".ascii \"clusterIdi\"\n"
   ".int32 25\n"
   ".ascii \"umbra_wasmudo_KMeans_init\"\n"
   ".int32 28\n"
   ".ascii \"umbra_wasmudo_KMeans_destroy\"\n"
   ".int32 27\n"
   ".ascii \"umbra_wasmudo_KMeans_accept\"\n"
   ".int32 31\n"
   ".ascii \"umbra_wasmudo_KMeans_extra_work\"\n"
   ".int32 28\n"
   ".ascii \"umbra_wasmudo_KMeans_process\"\n"
   ".int32 25\n"
   ".ascii \"umbra_wasmudo_KMeans_emit\"\n"
);
//---------------------------------------------------------------------------
