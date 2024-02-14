#ifndef H_udo_runtime_UDOperator
#define H_udo_runtime_UDOperator
//---------------------------------------------------------------------------
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
//---------------------------------------------------------------------------
namespace udo {
//---------------------------------------------------------------------------
/// Print a debug message
void printDebug(const char* msg, uint64_t size);
//---------------------------------------------------------------------------
/// Print a debug message
inline void printDebug(std::string_view message) {
   printDebug(message.data(), message.size());
}
//---------------------------------------------------------------------------
/// Get a random number
uint64_t getRandom();
//---------------------------------------------------------------------------
/// The data128 type used for strings
struct data128_t {
   uint64_t values[2];
};
//---------------------------------------------------------------------------
/// A string value than can be used in a tuple
class String {
   private:
   /// The string data
   data128_t stringData = {};

   /// The short string limit
   static constexpr uint32_t shortStringLimit = 12;

   public:
   /// Default constructor
   String() = default;

   /// Get the size of the string
   uint32_t size() const {
      uint32_t s;
      std::memcpy(&s, &stringData, sizeof(uint32_t));
      return s;
   }

   /// Get the pointer to the string
   const char* data() const {
      if (size() <= shortStringLimit) {
         return reinterpret_cast<const char*>(reinterpret_cast<const char*>(&stringData) + sizeof(uint32_t));
      } else {
         uintptr_t rawPtr = stringData.values[1];
         rawPtr &= ~(0b11ull << 62);
         return reinterpret_cast<const char*>(rawPtr);
      }
   }

   /// Implicit conversion from string_view
   String(std::string_view sv) {
      uint32_t size = sv.size();
      std::memcpy(&stringData, &size, sizeof(uint32_t));
      if (size <= shortStringLimit) {
         std::memcpy(reinterpret_cast<char*>(&stringData) + sizeof(uint32_t), sv.data(), size);
      } else {
         std::memset(reinterpret_cast<char*>(&stringData) + sizeof(uint32_t), 0, sizeof(uint32_t));
         uintptr_t rawPtr = reinterpret_cast<uintptr_t>(sv.data());
         rawPtr |= 1ull << 62;
         stringData.values[1] = rawPtr;
      }
   }

   /// Implicit conversion to string_view
   operator std::string_view() const {
      return {data(), size()};
   }
};
//---------------------------------------------------------------------------
struct EmptyTuple {
};
//---------------------------------------------------------------------------
/// The local state each worker can use.
struct LocalState {
   /// The actual data. It is aligned to 16B and is set to zero initially.
   alignas(16) std::byte data[16];
};
//---------------------------------------------------------------------------
/// The execution state of a UDO
class ExecutionState {
   private:
   /// The internal state
   void* data[2];

   public:
   ExecutionState() = delete;
   ExecutionState(const ExecutionState&) = default;
   ExecutionState& operator=(const ExecutionState&) = default;

   /// Get the thread id of the current thread
   uint32_t getThreadId();

   /// Get the local state for the thread of this execution state
   LocalState& getLocalState();
};
//---------------------------------------------------------------------------
class UDOperator {
   public:
   /// The value returned by extraWork() when all work is done
   static constexpr uint32_t extraWorkDone = -1;

   /// Get the thread id from an execution state
   static uint32_t getThreadId(ExecutionState executionState) {
      return executionState.getThreadId();
   }

   /// Get the local state from an execution state
   static LocalState& getLocalState(ExecutionState executionState) {
      return executionState.getLocalState();
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
