#ifndef H_udo_umbra_wasmudo
#define H_udo_umbra_wasmudo
//---------------------------------------------------------------------------
#include <stdint.h>
//---------------------------------------------------------------------------
// Umbra
// (c) 2022 Moritz Sichert
//---------------------------------------------------------------------------
#ifdef __cplusplus
extern "C" {
#endif
//---------------------------------------------------------------------------
typedef struct {
   __attribute__((aligned(16))) char data[16];
} umbra_wasmudo_local_state;
//---------------------------------------------------------------------------
typedef __externref_t umbra_wasmudo_execution_state;
//---------------------------------------------------------------------------
/// Get the thread id
__attribute__((import_name("umbra_wasmudo_get_thread_id"))) uint32_t umbra_wasmudo_get_thread_id(umbra_wasmudo_execution_state executionState);
//---------------------------------------------------------------------------
/// Get the pointer to the local state
__attribute__((import_name("umbra_wasmudo_get_local_state"))) umbra_wasmudo_local_state* umbra_wasmudo_get_local_state(umbra_wasmudo_execution_state executionState);
//---------------------------------------------------------------------------
/// Get a random number
__attribute__((import_name("umbra_wasmudo_get_random"))) uint64_t umbra_wasmudo_get_random();
//---------------------------------------------------------------------------
typedef __externref_t umbra_wasmudo_string;
//---------------------------------------------------------------------------
/// Get the length of a string
__attribute__((import_name("umbra_wasmudo_string_length"))) uint32_t umbra_wasmudo_string_length(umbra_wasmudo_string str);
//---------------------------------------------------------------------------
/// Extract the bytes of a string starting from a offset into a buffer of given size
__attribute__((import_name("umbra_wasmudo_extract_string"))) void umbra_wasmudo_extract_string(umbra_wasmudo_string str, uint32_t offset, char* buffer, uint32_t bufferSize);
//---------------------------------------------------------------------------
/// Create a string that can be passed to the emit function
__attribute__((import_name("umbra_wasmudo_create_string"))) umbra_wasmudo_string umbra_wasmudo_create_string(const char* str, uint32_t size);
//---------------------------------------------------------------------------
#ifdef __cplusplus
}
#endif
//---------------------------------------------------------------------------
#endif
