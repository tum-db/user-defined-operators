#ifndef H_udo_standalone_util
#define H_udo_standalone_util
//---------------------------------------------------------------------------
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <concepts>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string_view>
#include <thread>
#include <tuple>
#include <type_traits>
#include <udo/ChunkedStorage.hpp>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <sched.h>
#include <sys/stat.h>
#include <unistd.h>
//---------------------------------------------------------------------------
namespace udo_util {
//---------------------------------------------------------------------------
inline size_t getNumThreads()
// Get the number of available threads
{
   ::cpu_set_t cpuSet = {};
   if (::sched_getaffinity(0, sizeof(cpuSet), &cpuSet) != 0)
      return ~0ull;

   size_t threadCount = CPU_COUNT(&cpuSet);
   return threadCount;
}
//---------------------------------------------------------------------------
namespace impl {
//---------------------------------------------------------------------------
/// Parse a uint64_t from a string
inline void parseValue(uint64_t& value, std::string_view str) {
   auto result = std::from_chars(str.data(), str.data() + str.size(), value);
   if (result.ec != std::errc{})
      value = ~0ull;
}
//---------------------------------------------------------------------------
/// Parse a double from a string
inline void parseValue(double& value, std::string_view str) {
   auto result = std::from_chars(str.data(), str.data() + str.size(), value);
   if (result.ec != std::errc{})
      value = NAN;
}
//---------------------------------------------------------------------------
template <typename T>
std::string_view parseTuple(T&, std::string_view str, std::index_sequence<>)
// Recursion termination for `parseTuple`
{
   return str;
}
//---------------------------------------------------------------------------
template <typename T, std::size_t I, std::size_t... Is>
std::string_view parseTuple(T& tuple, std::string_view str, std::index_sequence<I, Is...>)
// Parse the Ith element of the tuple
{
   char delimiter;
   if (sizeof...(Is) == 0)
      // This is the last element, so we expect the newline at the end
      delimiter = '\n';
   else
      // Otherwise, the next element starts with the next comma
      delimiter = ',';

   auto pos = str.find(delimiter);

   using std::get;
   auto& elem = get<I>(tuple);

   parseValue(elem, str.substr(0, pos));

   str.remove_prefix(pos + 1);
   return parseTuple(tuple, str, std::index_sequence<Is...>{});
}
//---------------------------------------------------------------------------
inline ssize_t preadLoop(int fd, void* buf, size_t count, off_t offset)
// Wrapper around `pread` that only returns if all bytes were read
{
   size_t totalBytesRead = 0;
   size_t bytesRemaining = count;

   while (bytesRemaining > 0) {
      auto bytesRead = ::pread(fd, static_cast<char*>(buf) + totalBytesRead, bytesRemaining, offset + totalBytesRead);
      if (bytesRead <= 0)
         return bytesRead;
      totalBytesRead += bytesRead;
      bytesRemaining -= bytesRead;
   }

   return totalBytesRead;
}
//---------------------------------------------------------------------------
template <typename InputTuple, std::size_t... Is>
udo::ParallelChunkedStorage<InputTuple> parseCsvImpl(std::string_view fileName, std::index_sequence<Is...>)
// Implementation for `parseCsv`
{
   int inputFileFd = ::open(fileName.data(), O_RDONLY | O_CLOEXEC);
   if (inputFileFd < 0) {
      std::cerr << "Failed opening " << fileName << ": " << std::strerror(errno) << std::endl;
      std::exit(1);
   }

   struct ::stat fileStat {};
   if (::fstat(inputFileFd, &fileStat) < 0) {
      std::cerr << "stat(" << fileName << ") failed: " << std::strerror(errno) << std::endl;
      std::exit(1);
   }

   if (!(S_ISREG(fileStat.st_mode) || S_ISBLK(fileStat.st_mode))) {
      std::cerr << fileName << " has unsupported file type, should be regular file or block device" << std::endl;
      std::exit(1);
   }

   size_t fileSize = fileStat.st_size;

   static constexpr size_t pageSize = 4096;
   static constexpr size_t sizePerThread = pageSize * 16;

   std::atomic<size_t> currentOffset = 0;

   uint32_t numThreads = getNumThreads();

   udo::ParallelChunkedStorage<InputTuple> input;
   std::vector<std::thread> threads;

   for (uint32_t threadId = 0; threadId < numThreads; ++threadId) {
      threads.emplace_back([&, threadId, inputFileFd, fileSize] {
         auto chunkRef = input.createLocalStorage(threadId);

         // Read one additional page so that we can read until the end of the
         // last line
         auto buffer = std::make_unique<char[]>(sizePerThread + pageSize);
         std::string_view inputData(buffer.get(), sizePerThread + pageSize);

         while (true) {
            size_t localOffset = currentOffset.fetch_add(sizePerThread);
            if (localOffset >= fileSize)
               break;

            size_t maxBytesToRead = fileSize - localOffset;
            auto bytesToRead = std::min<size_t>(sizePerThread + pageSize, maxBytesToRead);
            if (preadLoop(inputFileFd, buffer.get(), bytesToRead, localOffset) < 0)
               break;

            auto forwardToNextLine = [&](size_t offset) -> size_t {
               size_t newlineOffset = inputData.find('\n', offset);
               if (newlineOffset == std::string_view::npos)
                  return inputData.size();
               else
                  return newlineOffset + 1;
            };

            // This will skip over the header line in the thread that reads the
            // first chunk of the file, and correctly skip the last line that
            // was already read in all other chunks.
            size_t offsetBegin = forwardToNextLine(0);
            size_t offsetEnd;
            if (maxBytesToRead < sizePerThread)
               // We are at the end of the file, so we read it until the end
               offsetEnd = maxBytesToRead;
            else
               offsetEnd = forwardToNextLine(sizePerThread);

            std::string_view inputStr = inputData.substr(offsetBegin, offsetEnd - offsetBegin);

            while (!inputStr.empty()) {
               InputTuple in;
               inputStr = parseTuple(in, inputStr, std::index_sequence<Is...>{});
               chunkRef->push_back(in);
            }
         }
      });
   }

   for (auto& thread : threads)
      thread.join();

   ::close(inputFileFd);

   return input;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
template <typename InputTuple>
udo::ParallelChunkedStorage<InputTuple> parseCsv(std::string_view fileName)
/// Parse a csv that contains tuples of the given type
{
   return impl::parseCsvImpl<InputTuple>(fileName, std::make_index_sequence<std::tuple_size_v<InputTuple>>{});
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
#endif
