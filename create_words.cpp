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
/// The words that will be selected randomly. Words are taken from "Topics of
/// Interest" at http://vldb.org/pvldb/vol15-contributions/
static constexpr array words = {
   "Data Mining and Analytics"sv,
   "Data Warehousing, OLAP, Parallel and Distributed Data Mining"sv,
   "Mining and Analytics for Scientific and Business data, Social Networks, Time Series, Streams, Text, Web, Graphs, Rules, Patterns, Logs, and Spatio-temporal Data"sv,
   "Data Privacy and Security"sv,
   "Blockchain"sv,
   "Access Control and Privacy"sv,
   "Database Engines"sv,
   "Access Methods, Concurrency Control, Recovery and Transactions"sv,
   "Hardware Accelerators"sv,
   "Query Processing and Optimization"sv,
   "Storage Management, Multi-core Databases, In-memory Data Management"sv,
   "Views, Indexing and Search"sv,
   "Database Performance"sv,
   "Tuning, Benchmarking and Performance Measurement"sv,
   "Administration and Manageability"sv,
   "Distributed Database Systems"sv,
   "Content Delivery Networks, Database-as-a-service, and Resource Management"sv,
   "Cloud Data Management"sv,
   "Distributed Analytics"sv,
   "Distributed Transactions"sv,
   "Graphs, Networks, and Semistructured Data"sv,
   "Graph Data Management, Recommendation Systems, Social Networks"sv,
   "Hierarchical, Non-relational, and other Modern Data Models"sv,
   "Information Integration and Data Quality"sv,
   "Data Cleaning, Data Discovery and Data Exploration"sv,
   "Heterogeneous and Federated DBMS, Metadata Management"sv,
   "Web Data Management and Semantic Web"sv,
   "Knowledge Graphs and Knowledge Management"sv,
   "Languages"sv,
   "Data Models and Query Languages"sv,
   "Schema Management and Design"sv,
   "Machine Learning, AI and Databases"sv,
   "Data Management Issues and Support for Machine Learning and AI"sv,
   "Machine Learning and Applied AI for Data Management"sv,
   "Novel DB Architectures"sv,
   "Embedded and Mobile Databases"sv,
   "Data management on novel hardware"sv,
   "Real-time databases, Sensors and IoT, Stream Databases"sv,
   "Crowd-sourcing"sv,
   "Provenance and Workflows"sv,
   "Profile-based and Context-Aware Data Management"sv,
   "Process Mining"sv,
   "Provenance analytics"sv,
   "Debugging"sv,
   "Specialized and Domain-Specific Data Management"sv,
   "Spatial Databases and Temporal Databases"sv,
   "Crowdsourcing"sv,
   "Ethical Data Management"sv,
   "Fuzzy, Probabilistic and Approximate Data"sv,
   "Image and Multimedia Databases"sv,
   "Scientific and Medical Data Management"sv,
   "Text, Semi-Structured Data, and IR"sv,
   "Information Retrieval"sv,
   "Text in Databases"sv,
   "Data Extraction"sv,
   "User Interfaces"sv,
   "Database Usability"sv,
   "Database support for Visual Analytics"sv,
   "Visualization"sv,
};
//---------------------------------------------------------------------------
/// The output of this operator
struct Output {
   udo::String word;
};
//---------------------------------------------------------------------------
class CreateWords : public udo::UDOperator<udo::EmptyTuple, Output> {
   private:
   /// The total number of words that should be generated
   uint64_t numWords;
   /// The counter to track the number of words that were generated
   atomic<uint64_t> wordCount = 0;

   public:
   /// Constructor
   explicit CreateWords(uint64_t numWords) : numWords(numWords) {}

   /// Produce the output
   bool postProduce(LocalState& /*localState*/) {
      uint64_t localWordCount = wordCount.fetch_add(10000);
      if (localWordCount >= numWords)
         return true;

      uint64_t seed = 42 + localWordCount;
      mt19937_64 gen(seed);
      uniform_int_distribution<size_t> wordIndexDistr(0, words.size() - 1);
      uniform_int_distribution<uint32_t> randomNumberDistr;

      for (uint64_t i = 0; i < 10000 && localWordCount + i < numWords; ++i) {
         auto baseWord = words[wordIndexDistr(gen)];
         // Add a random number as prefix and suffix to the string so that it's
         // not just a bunch of identical strings.
         string word = to_string(randomNumberDistr(gen));
         word += ' ';
         word += baseWord;
         word += ' ';
         word += to_string(randomNumberDistr(gen));

         Output output;
         output.word = string_view(word);

         produceOutputTuple(output);
      }


      return false;
   }
};
//---------------------------------------------------------------------------
