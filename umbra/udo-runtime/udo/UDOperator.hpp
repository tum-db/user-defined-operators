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
void printDebug(std::string_view message) {
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
template <typename IT, typename OT>
class UDOperator {
   public:
   /// The input tuple type
   using InputTuple = IT;
   /// The output tuple type
   using OutputTuple = OT;

   /// The local state each worker can use.
   struct LocalState {
      /// The actual data. It is aligned to 16B and is set to zero initially.
      alignas(16) std::byte data[16];
   };

   /// The value returned by extraWork() when all work is done
   static constexpr uint32_t extraWorkDone = -1;

   /// Produce a tuple as output
   static void produceOutputTuple(const OutputTuple& output) noexcept;

   /// Accept an incoming tuple
   void consume(LocalState& /*localState*/, const InputTuple& /*input*/) {}

   /// Do some extra work after all input tuples were consumed
   uint32_t extraWork(LocalState& /*localState*/, uint32_t /*stepId*/) { return extraWorkDone; }

   /// Do work after all tuples were consumed and generate the output
   bool postProduce(LocalState& /*localState*/) { return true; }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
#endif
