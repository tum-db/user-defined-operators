#ifndef H_udo_UDOperator
#define H_udo_UDOperator
//---------------------------------------------------------------------------
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <udo/umbra_wasmudo.h>
//---------------------------------------------------------------------------
namespace udo {
//---------------------------------------------------------------------------
uint64_t getRandom()
// Get a random number
{
   return umbra_wasmudo_get_random();
}
//---------------------------------------------------------------------------
class alignas(std::string) String {
   private:
   /// The maximum size for an inline string
   static constexpr uint32_t inlineStringLimit = 128 - sizeof(uint32_t);

   /// The size of the string
   uint32_t strSize = 0;
   /// The string data. Either the actual string if the size is smaller than
   /// the inline string limit, or a pointer to a std::string.
   char strData[inlineStringLimit];

   /// Get the pointer to the out-of-line string
   std::string* getOutOfLineString() {
      return reinterpret_cast<std::string*>(strData + (alignof(std::string) - sizeof(uint32_t)));
   }
   /// Get the pointer to the out-of-line string
   const std::string* getOutOfLineString() const {
      return const_cast<String*>(this)->getOutOfLineString();
   }

   public:
   /// Default constuctor
   String() = default;

   /// Construct from a wasmudo string
   explicit String(umbra_wasmudo_string rawStr) {
      strSize = umbra_wasmudo_string_length(rawStr);
      if (strSize <= inlineStringLimit) {
         umbra_wasmudo_extract_string(rawStr, 0, strData, strSize);
      } else {
         auto* str = std::construct_at(getOutOfLineString());
         str->resize(strSize);
         umbra_wasmudo_extract_string(rawStr, 0, str->data(), strSize);
      }
   }

   /// Implicit conversion from a string view
   String(std::string_view str) {
      strSize = str.size();
      if (strSize <= inlineStringLimit)
         std::memcpy(strData, str.data(), strSize);
      else
         std::construct_at(getOutOfLineString(), str);
   }

   /// Destructor
   ~String() {
      clear();
   }

   /// Move constructor
   String(String&& other) noexcept {
      *this = std::move(other);
   }

   /// Move assignment
   String& operator=(String&& other) noexcept {
      if (this == &other)
         return *this;

      clear();
      strSize = other.strSize;
      if (strSize <= inlineStringLimit)
         std::memcpy(strData, other.strData, strSize);
      else
         std::construct_at(getOutOfLineString(), std::move(*other.getOutOfLineString()));
      other.clear();

      return *this;
   }

   /// Get the size of the string
   uint32_t size() const {
      return strSize;
   }

   /// Get the pointer to the string
   const char* data() const {
      if (strSize <= inlineStringLimit)
         return strData;
      else
         return getOutOfLineString()->data();
   }

   /// Clear the string
   void clear() {
      if (strSize > inlineStringLimit)
         std::destroy_at(getOutOfLineString());
      strSize = 0;
   }

   /// Get this string as a raw wasmudo string
   umbra_wasmudo_string asRaw() const {
      return umbra_wasmudo_create_string(data(), size());
   }

   /// Implicit conversion to string_view
   operator std::string_view() const {
      return std::string_view(data(), size());
   }
};
//---------------------------------------------------------------------------
using ExecutionState = umbra_wasmudo_execution_state;
//---------------------------------------------------------------------------
struct EmptyTuple {};
//---------------------------------------------------------------------------
using LocalState = umbra_wasmudo_local_state;
//---------------------------------------------------------------------------
class UDOperator {
   public:
   /// The value returned by extraWork() when all work is done
   static constexpr uint32_t extraWorkDone = -1;

   /// Get the thread id from the execution state
   static uint32_t getThreadId(ExecutionState executionState) {
      return umbra_wasmudo_get_thread_id(executionState);
   }

   /// Get the local state from the execution state
   static LocalState& getLocalState(ExecutionState executionState) {
      return *umbra_wasmudo_get_local_state(executionState);
   }

   /// Emit a tuple of the output
   template <typename Derived>
      requires std::is_base_of_v<UDOperator, Derived>
   static void emit(ExecutionState executionState, const typename Derived::OutputTuple& output) noexcept;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
#endif
