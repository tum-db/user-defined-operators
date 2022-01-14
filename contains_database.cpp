#include <string_view>
//---------------------------------------------------------------------------
#include <udo/UDOperator.hpp>
//---------------------------------------------------------------------------
using namespace std;
using namespace std::literals::string_view_literals;
//---------------------------------------------------------------------------
struct Tuple {
   udo::String word;
};
//---------------------------------------------------------------------------
class ContainsDatabase : public udo::UDOperator<Tuple, Tuple> {
   /// The word "database" that we are looking for
   static constexpr string_view databaseLower = "database"sv;
   /// The upper case letters for the database word
   static constexpr string_view databaseUpper = "DATABASE"sv;

   public:
   /// Search for the word database, case-insensitively, by using a KMP search
   /// and only produce the tuple if the word was found.
   void consume(LocalState& /*localState*/, const Tuple& input) {
      string_view word = input.word;

      // The current index in the input word
      size_t currentIndex = 0;
      // The current index in the pattern (i.e. the database word)
      size_t patternIndex = 0;

      while (currentIndex < word.size()) {
         if (word[currentIndex] == databaseLower[patternIndex] || word[currentIndex] == databaseUpper[patternIndex]) {
            ++currentIndex;
            ++patternIndex;

            if (patternIndex == databaseLower.size()) {
               // We found a match
               produceOutputTuple(input);
               break;
            }
         } else {
            // We know that no substrings of the word database are prefixes of
            // the word itself, so instead of needing a pre-processing step as
            // usual in KMP, we can just directly continue with the current
            // index and just reset the pattern.
            if (patternIndex == 0) {
               // We are at pattern index 0 and this wasn't a match, so we need
               // to increase the current index into the string.
               ++currentIndex;
            }
            patternIndex = 0;
         }
      }
   }
};
//---------------------------------------------------------------------------
