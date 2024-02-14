#include <array>
#include <atomic>
#include <memory>
#include <string_view>
#ifdef UDO_STANDALONE
#include "standalone_util.hpp"
#include <chrono>
#include <udo/UDOStandalone.hpp>
#endif
//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
using namespace std::literals::string_view_literals;
//---------------------------------------------------------------------------
/// The linear regression operator. It solves the following problem:
/// y_i = a + bx_i + cx_i^2
/// Determine a, b and c for the given values for x and y while minimizing the
/// sum of the squared errors:
/// Sum_i (a + bx_i + cx_i^2 - y_i)^2
///
/// This can be solved as follows:
///
/// Setting the partial derivatives of the squared errors to 0 results in:
///
/// /                           \   /   \   /          \_
/// | Sum 1    Sum x    Sum x^2 |   | a |   | Sum y    |
/// | Sum x    Sum x^2  Sum x^3 | * | b | = | Sum xy   |
/// | Sum x^2  Sum x^3  Sum x^4 |   | c |   | Sum x^2y |
/// \                           /   \   /   \          /
///
/// To calculate a, b, and c we calculate the inverse of the first matrix and
/// multiply it from the left:
///
/// /   \   /                           \-1  /          \_
/// | a |   | Sum 1    Sum x    Sum x^2 |    | Sum y    |
/// | b | = | Sum x    Sum x^2  Sum x^3 |  * | Sum xy   |
/// | c |   | Sum x^2  Sum x^3  Sum x^4 |    | Sum x^2y |
/// \   /   \                           /    \          /
///
/// This results in a closed form solution for a, b, and c:
///
/// a = 1 / det(A) * (
///        Sum y (Sum x^2 Sum x^4 - (Sum x^3)^2) +
///        Sum xy (Sum x^2 Sum x^3 - Sum x Sum x^4) +
///        Sum x^2y (Sum x Sum x^3 - (Sum x^2)^2)
///     )
///
/// b = 1 / det(A) * (
///        Sum y (Sum x^2 Sum x^3 - Sum x Sum x^4) +
///        Sum xy (Sum 1 Sum x^4 - (Sum x^2)^2) +
///        Sum x^2y (Sum x Sum x^2 - Sum 1 Sum x^3)
///     )
///
/// c = 1 / det(A) * (
///        Sum y (Sum x Sum x^3 - (Sum x^2)^2) +
///        Sum xy (Sum x Sum x^2 - Sum 1 Sum x^3) +
///        Sum x^2y (Sum 1 Sum x^2 - (Sum x)^2)
///     )
///
/// with det(A) =
///      Sum 1 Sum x^2 Sum x^4
///      + 2 Sum x Sum x^2 Sum x^3
///      - (Sum x^2)^3
///      - Sum 1 (Sum x^3)^2
///      - (Sum x)^2 Sum x^4
///
/// Since everything we calculate are sums, we can trivially parallelize this by
/// letting each thread calculate the partial sums of the values it receives and
/// then sum up all partial sums once at the end. With the partial sums we then
/// determine det(A) and finally a, b, and c.
class LinearRegression : public udo::UDOperator {
   public:
   struct InputTuple {
      // The value for x
      double x;
      // The measurement of y that will be fitted
      double y;

      template <size_t I>
         requires(I < 2)
      double& get() {
         if constexpr (I == 0)
            return x;
         else if constexpr (I == 1)
            return y;
      }
   };

   struct OutputTuple {
      // The value for the parameter a
      double a;
      // The value for the parameter b
      double b;
      // The value for the parameter c
      double c;
   };

   private:
   /// The partial sums of a thread
   struct alignas(64) PartialSums {
      // The value for Sum 1
      double sum1 = 0.0;
      // The value for Sum x
      double sumx = 0.0;
      // The value for Sum x^2
      double sumx2 = 0.0;
      // The value for Sum x^3
      double sumx3 = 0.0;
      // The value for Sum x^4
      double sumx4 = 0.0;
      // The value for Sum y
      double sumy = 0.0;
      // The value for Sum xy
      double sumxy = 0.0;
      // The value for Sum x^2y
      double sumx2y = 0.0;
   };

   /// The local state of a thread in the regression
   struct RegressionLocalState {
      /// The partial sums
      PartialSums partialSums;
      /// The pointer to the next local state
      RegressionLocalState* next = nullptr;
   };

   /// The list of local states
   atomic<RegressionLocalState*> localStateList = nullptr;
   /// The mutex flag to return the result
   atomic_flag resultMutex = false;

#ifndef WASMUDO
   /// Get the local state from an execution state
   static udo::LocalState& getLocalState(udo::ExecutionState executionState) {
      return executionState.getLocalState();
   }
#endif

   public:
   /// Accept an input tuple
   void accept(udo::ExecutionState executionState, const InputTuple& input) {
      auto*& localState = reinterpret_cast<RegressionLocalState*&>(getLocalState(executionState));
      if (!localState) {
         auto newLocalState = make_unique<RegressionLocalState>();
         newLocalState->next = localStateList.load();
         while (!localStateList.compare_exchange_weak(newLocalState->next, newLocalState.get()))
            ;

         localState = newLocalState.get();
         // This will be deallocated in process()
         newLocalState.release();
      }

      double x = input.x;
      double y = input.y;

      auto x2 = x * x;
      auto x3 = x2 * x;
      auto x4 = x2 * x2;
      auto xy = x * y;
      auto x2y = x2 * y;

      auto& sums = localState->partialSums;
      sums.sum1 += 1;
      sums.sumx += x;
      sums.sumx2 += x2;
      sums.sumx3 += x3;
      sums.sumx4 += x4;
      sums.sumy += y;
      sums.sumxy += xy;
      sums.sumx2y += x2y;
   }

   /// Produce the output
   bool process(udo::ExecutionState executionState) {
      if (resultMutex.test_and_set())
         return true;

      // Sum up all partial sums from the local states
      PartialSums sums;
      for (auto* localState = localStateList.load(); localState;) {
         unique_ptr<RegressionLocalState> localStatePtr(localState);

         auto& lsums = localStatePtr->partialSums;
         sums.sum1 += lsums.sum1;
         sums.sumx += lsums.sumx;
         sums.sumx2 += lsums.sumx2;
         sums.sumx3 += lsums.sumx3;
         sums.sumx4 += lsums.sumx4;
         sums.sumy += lsums.sumy;
         sums.sumxy += lsums.sumxy;
         sums.sumx2y += lsums.sumx2y;

         localState = localStatePtr->next;
      }

      // clang-format off
      double detInv = 1 / (
         sums.sum1 * sums.sumx2 * sums.sumx4
         + 2 * sums.sumx * sums.sumx2 * sums.sumx3
         - sums.sumx2 * sums.sumx2 * sums.sumx2
         - sums.sum1 * sums.sumx3 * sums.sumx3
         - sums.sumx * sums.sumx * sums.sumx4
      );
      double a = detInv * (
         sums.sumy * (sums.sumx2 * sums.sumx4 - sums.sumx3 * sums.sumx3)
         + sums.sumxy * (sums.sumx2 * sums.sumx3 - sums.sumx * sums.sumx4)
         + sums.sumx2y * (sums.sumx * sums.sumx3 - sums.sumx2 * sums.sumx2)
      );
      double b = detInv * (
         sums.sumy * (sums.sumx2 * sums.sumx3 - sums.sumx * sums.sumx4)
         + sums.sumxy * (sums.sum1 * sums.sumx4 - sums.sumx2 * sums.sumx2)
         + sums.sumx2y * (sums.sumx * sums.sumx2 - sums.sum1 * sums.sumx3)
      );
      double c = detInv * (
         sums.sumy * (sums.sumx * sums.sumx3 - sums.sumx2 * sums.sumx2)
         + sums.sumxy * (sums.sumx * sums.sumx2 - sums.sum1 * sums.sumx3)
         + sums.sumx2y * (sums.sum1 * sums.sumx2 - sums.sumx * sums.sumx)
      );
      // clang-format on

      emit<LinearRegression>(executionState, {a, b, c});

      return true;
   }
};
//---------------------------------------------------------------------------
template <size_t I>
decltype(auto) get(LinearRegression::InputTuple& tuple) {
   return tuple.get<I>();
}
//---------------------------------------------------------------------------
namespace std {
//---------------------------------------------------------------------------
template <>
struct tuple_size<LinearRegression::InputTuple> : std::integral_constant<std::size_t, 2> {};
//---------------------------------------------------------------------------
template <std::size_t I>
struct tuple_element<I, LinearRegression::InputTuple> {
   using type = double;
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
#ifdef WASMUDO
// plugin-wasmudo -generate-cxx-header -no-init -no-destroy LinearRegression 16 8 '' f64,f64 'a f64,b f64,c f64' > wasmudo_udo_regression.hpp
#include "wasmudo_udo_regression.hpp"
#endif
//---------------------------------------------------------------------------
#ifdef UDO_STANDALONE
//---------------------------------------------------------------------------
int main(int argc, const char** argv) {
   bool argError = false;
   bool benchmark = false;
   string_view inputFileName;

   const char** argIt = argv;
   ++argIt;
   const char** argEnd = argv + argc;
   for (; argIt != argEnd; ++argIt) {
      string_view arg(*argIt);
      if (arg.empty())
         continue;
      if (arg == "--benchmark") {
         benchmark = true;
      } else {
         if (inputFileName.empty()) {
            inputFileName = arg;
         } else {
            argError = true;
            break;
         }
      }
   }

   if (!argError && inputFileName.empty())
      argError = true;

   if (argError) {
      cerr << "Usage: " << argv[0] << " [--benchmark] <input file>" << endl;
      return 2;
   }

   auto startParse = chrono::steady_clock::now();
   auto input = udo_util::parseCsv<LinearRegression::InputTuple>(inputFileName);
   auto endParse = chrono::steady_clock::now();

   auto numThreads = udo_util::getNumThreads();

   if (benchmark) {
      // We immediately discard the input that was just parsed above. We do
      // this so that the operating system has the chance to cache the input
      // file before we start the measurements.
      input.clear();

      for (unsigned i = 0; i < 3; ++i) {
         auto startParse = chrono::steady_clock::now();
         auto input = udo_util::parseCsv<LinearRegression::InputTuple>(inputFileName);
         auto endParse = chrono::steady_clock::now();

         auto parseNs = chrono::duration_cast<chrono::nanoseconds>(endParse - startParse).count();
         cout << "parse:" << parseNs << '\n';

         for (unsigned j = 0; j < 6; ++j) {
            udo::UDOStandalone<LinearRegression> standalone(numThreads, 10000);
            LinearRegression regression;

            auto start = chrono::steady_clock::now();
            auto output = standalone.run(regression, input);
            auto end = chrono::steady_clock::now();
            auto durationNs = chrono::duration_cast<chrono::nanoseconds>(end - start).count();
            // Don't measure the first run
            if (j > 0)
               cout << "exec:" << durationNs << '\n';
         }
      }
   } else {
      auto durationMs = chrono::duration_cast<chrono::milliseconds>(endParse - startParse).count();
      auto numTuples = input.size();
      cout << "Parsing: " << durationMs << " ms, " << numTuples << " tuples" << endl;

      udo::UDOStandalone<LinearRegression> standalone(numThreads, 10000);
      LinearRegression regression;
      auto output = standalone.run(regression, input);

      auto& params = *output.begin();
      cout << "a = " << params.a << '\n';
      cout << "b = " << params.b << '\n';
      cout << "c = " << params.c << '\n';
      cout << "-> y = " << params.a << " + " << params.b << "x"
           << " + " << params.c << "x^2\n";
   }

   return 0;
}
//---------------------------------------------------------------------------
#endif
