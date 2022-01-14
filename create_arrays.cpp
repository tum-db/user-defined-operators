#include <array>
#include <atomic>
#include <random>
#include <string>
#include <string_view>
//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
using namespace std::literals::string_view_literals;
//---------------------------------------------------------------------------
/// The names that are randomly selected for the name attribute
static constexpr array names = {
    "DuckDB"sv,
    "Hyper"sv,
    "MSSQL"sv,
    "MonetDB"sv,
    "Peloton"sv,
    "Postgres"sv,
    "Umbra"sv,
    "Vectorwise"sv,
};
//---------------------------------------------------------------------------
/// The strings that are used for "invalid" values
static constexpr array invalidValues = {
    ""sv,
    "F"sv,
    "FALSE"sv,
    "N/A"sv,
    "NaN"sv,
    "f"sv,
    "false"sv,
    "n/a"sv,
    "nan"sv,
};
//---------------------------------------------------------------------------
/// The output of this operator
struct Output {
   udo::String name;
   udo::String values;
};
//---------------------------------------------------------------------------
class CreateArrays : public udo::UDOperator<udo::EmptyTuple, Output> {
   private:
   /// The total number of tuples that should be generated
   uint64_t numTuples;
   /// The counter to track the number of tuples that were generated
   atomic<uint64_t> tupleCount = 0;

   public:
   /// Constructor
   explicit CreateArrays(uint64_t numTuples) : numTuples(numTuples) {}

   /// Produce the output
   bool postProduce(LocalState& /*localState*/) {
      uint64_t localTupleCount = tupleCount.fetch_add(10000);
      if (localTupleCount >= numTuples)
         return true;

      uint64_t seed = 42 + localTupleCount;
      mt19937_64 gen(seed);
      uniform_int_distribution<size_t> nameIndexDistr(0, names.size() - 1);
      uniform_int_distribution<size_t> invalidValueIndexDistr(0, invalidValues.size() - 1);
      bernoulli_distribution hasValueDistr(0.9);
      binomial_distribution<unsigned> numValuesDistr(50, 0.2);
      uniform_int_distribution<int> randomNumberDistr(0, 1000000);

      for (uint64_t i = 0; i < 10000 && localTupleCount + i < numTuples; ++i) {
         auto name = names[nameIndexDistr(gen)];

         string values;
         auto numValues = numValuesDistr(gen);
         for (unsigned j = 0; j < numValues; ++j) {
            if (j > 0)
                values += ',';
            if (hasValueDistr(gen)) {
                auto value = randomNumberDistr(gen);
                values += to_string(value);
            } else {
                auto value = invalidValues[invalidValueIndexDistr(gen)];
                values += value;
            }
         }

         Output output;
         output.name = name;
         output.values = string_view(values);

         produceOutputTuple(output);
      }


      return false;
   }
};
//---------------------------------------------------------------------------
