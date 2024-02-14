#ifndef H_udo_runtime_ChunkedStorage
#define H_udo_runtime_ChunkedStorage
//---------------------------------------------------------------------------
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iterator>
#include <new>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
//---------------------------------------------------------------------------
namespace udo {
//---------------------------------------------------------------------------
/// A container that has stable references, constant time insertion at the end
/// and allocates memory in exponentially increasing sizes.
template <typename T>
class ChunkedStorage {
   private:
   template <typename T2>
   friend class ParallelChunkedStorage;

   /// The header of a chunk. Make sure that this is aligned by at least the
   /// alignment of T so that the very first address after this is a valid
   /// address for T.
   struct alignas(std::max({alignof(T), alignof(size_t), alignof(void*)})) ChunkHeader {
      /// The total size of this chunk in bytes
      size_t size;
      /// The previous chunk in the list
      ChunkHeader* prev = nullptr;
      /// The next chunk in the list
      ChunkHeader* next = nullptr;
      /// The number of elements that are stored in this chunk
      size_t numElements = 0;

      /// Constructor from a size
      explicit ChunkHeader(size_t size) : size(size) {}

      /// Get the pointer to the first element
      T* getElements() {
         return reinterpret_cast<T*>(this + 1);
      }

      /// The maxmimum number of elements this chunk can hold
      size_t maxNumElements() const {
         return (size - sizeof(ChunkHeader)) / sizeof(T);
      }
   };

   /// The iterator
   template <bool isConst>
   class Iterator {
      public:
      using difference_type = std::ptrdiff_t;
      using value_type = std::conditional_t<isConst, const T, T>;
      using pointer = value_type*;
      using reference = value_type&;
      using iterator_category = std::forward_iterator_tag;

      private:
      friend class ChunkedStorage;

      /// The current chunk
      ChunkHeader* chunk = nullptr;
      /// The current index in the chunk
      size_t elementIndex = 0;

      /// Forward the iterator to the first non-empty chunk
      void forward() {
         while (chunk && chunk->numElements == 0)
            chunk = chunk->next;
      }

      /// Constructor
      Iterator(ChunkHeader* chunk, size_t elementIndex) : chunk(chunk), elementIndex(elementIndex) {
         forward();
      }

      public:
      /// Default constructor
      Iterator() = default;

      /// Dereference
      reference operator*() const {
         return chunk->getElements()[elementIndex];
      }
      /// Dereference
      pointer operator->() const {
         return &operator*();
      }

      /// Pre-increment
      Iterator& operator++() {
         ++elementIndex;
         if (elementIndex == chunk->numElements) {
            chunk = chunk->next;
            elementIndex = 0;
            forward();
         }
         return *this;
      }
      /// Post-increment
      Iterator operator++(int) {
         Iterator it(*this);
         operator++();
         return it;
      }

      /// Equality comparison
      bool operator==(const Iterator& other) const = default;
   };

   public:
   using value_type = T;
   using reference = T&;
   using const_reference = const T&;
   using iterator = Iterator<false>;
   using const_iterator = Iterator<true>;
   using difference_type = std::ptrdiff_t;
   using size_type = std::size_t;

   private:
   /// Get the minimum number of elements in a chunk. The size of a chunk
   /// should be at least 1024 bytes.
   static constexpr size_t minimumNumElements() {
      if (sizeof(ChunkHeader) + sizeof(T) >= 1024)
         return 1;

      return (1024 - sizeof(ChunkHeader) - 1) / sizeof(T) + 1;
   }

   /// Get the maxmimum number of elements in a chunk. The size of a chunk
   /// should not exceed 32 MiB.
   static constexpr size_t maximumNumElements() {
      return (32 * (1ull << 20) - sizeof(ChunkHeader)) / sizeof(T);
   }

   /// The first chunk
   ChunkHeader* frontChunk = nullptr;
   /// The last chunk
   ChunkHeader* backChunk = nullptr;
   /// The total number of elements
   size_t numElements = 0;

   /// Remove all elements and chunks
   void freeChunks() {
      auto* chunk = frontChunk;
      while (chunk) {
         auto* next = chunk->next;
         std::destroy_n(chunk->getElements(), chunk->numElements);
         std::free(chunk);
         chunk = next;
      }
      frontChunk = nullptr;
      backChunk = nullptr;
      numElements = 0;
   }

   /// Create a new chunk and append it at the end
   void addChunk() {
      size_t newChunkElements = std::max(numElements / 4, minimumNumElements());
      newChunkElements = std::min(newChunkElements, maximumNumElements());
      size_t newChunkSize = sizeof(ChunkHeader) + newChunkElements * sizeof(T);
      auto* chunkPtr = static_cast<ChunkHeader*>(std::malloc(newChunkSize));
      new (chunkPtr) ChunkHeader(newChunkSize);

      if (backChunk) {
         backChunk->next = chunkPtr;
         chunkPtr->prev = backChunk;
      } else {
         frontChunk = chunkPtr;
      }
      backChunk = chunkPtr;
   }

   public:
   /// Constructor
   ChunkedStorage() = default;

   /// Destructor
   ~ChunkedStorage() {
      freeChunks();
   }

   /// Move constructor
   ChunkedStorage(ChunkedStorage&& other) noexcept : frontChunk(other.frontChunk), backChunk(other.backChunk), numElements(other.numElements) {
      other.frontChunk = nullptr;
      other.backChunk = nullptr;
      other.numElements = 0;
   }

   /// Move assignment
   ChunkedStorage& operator=(ChunkedStorage&& other) noexcept {
      if (this == &other)
         return *this;

      freeChunks();

      frontChunk = other.frontChunk;
      backChunk = other.backChunk;
      numElements = other.numElements;
      other.frontChunk = nullptr;
      other.backChunk = nullptr;
      other.numElements = 0;

      return *this;
   }

   /// Get the number of elements stored in this ChunkedStorage
   size_type size() const { return numElements; }

   /// Is this storage empty?
   bool empty() const { return size() == 0; }

   /// Remove all elements
   void clear() {
      freeChunks();
   }

   /// Emplace a value at the end
   template <typename... Args>
   T& emplace_back(Args&&... args) {
      if (!backChunk || backChunk->numElements == backChunk->maxNumElements())
         addChunk();

      T* ptr = backChunk->getElements() + backChunk->numElements;
      new (ptr) T(std::forward<Args>(args)...);
      ++(backChunk->numElements);
      ++numElements;
      return *ptr;
   }

   /// Insert a value at the end
   T& push_back(const T& value) {
      return emplace_back(value);
   }

   /// Insert a value at the end
   T& push_back(T&& value) {
      return emplace_back(std::move(value));
   }

   /// Merge another ChunkedStorage into this
   void merge(ChunkedStorage&& other) noexcept {
      if (!other.frontChunk)
         return;
      if (!backChunk) {
         *this = std::move(other);
         return;
      }
      backChunk->next = other.frontChunk;
      backChunk = other.backChunk;
      numElements += other.numElements;
      other.frontChunk = nullptr;
      other.backChunk = nullptr;
      other.numElements = 0;
   }

   /// Get the iterator to the first element
   iterator begin() {
      return iterator(frontChunk, 0);
   }
   /// Get the iterator to the first element
   const_iterator begin() const {
      return const_iterator(frontChunk, 0);
   }
   /// Get the end iterator
   iterator end() {
      return iterator(nullptr, 0);
   }
   /// Get the end iterator
   const_iterator end() const {
      return iterator(nullptr, 0);
   }
};
//---------------------------------------------------------------------------
/// A collection of `ChunkedStorage` objects that supports efficient parallel
/// iteration.
template <typename T>
class ParallelChunkedStorage {
   public:
   /// A reference to a thread-local chunked storage
   class LocalChunkedStorageRef {
      private:
      friend class ParallelChunkedStorage;
      template <bool isConst>
      friend class ParallelIterator;

      /// The pointer to the chunked storage
      ChunkedStorage<T>* storage = nullptr;
      /// The thread index
      size_t index = 0;

      public:
      /// Dereference operator
      ChunkedStorage<T>& operator*() const {
         return *storage;
      }
      /// Dereference operator
      ChunkedStorage<T>* operator->() const {
         return storage;
      }

      /// Comparison operator
      bool operator==(const LocalChunkedStorageRef& other) const {
         return storage == other.storage;
      }
      /// Comparison with nullptr
      bool operator==(decltype(nullptr)) const {
         return storage == nullptr;
      }

      /// Explicit conversion to bool
      explicit operator bool() const {
         return storage;
      }
      /// Negation
      bool operator!() const {
         return !storage;
      }
   };

   private:
   using ChunkHeader = typename ChunkedStorage<T>::ChunkHeader;

   /// The chunked storage for one thread
   struct LocalChunkedStorageEntry {
      /// The chunked storage
      ChunkedStorage<T> storage;
      /// The thread id given by a caller of `createLocalStorage`
      uint32_t threadId;
      /// The index of this entry. All indexes are unique but may not be
      /// consistent with the order of the `next` pointers.
      size_t index = -1;
      /// The next entry
      LocalChunkedStorageEntry* next = nullptr;
   };


   /// An iterator over all elements in a `ParallelChunkedStorage`
   template <bool isConst>
   class Iterator {
      public:
      using difference_type = std::ptrdiff_t;
      using value_type = std::conditional_t<isConst, const T, T>;
      using pointer = value_type*;
      using reference = value_type&;
      using iterator_category = std::forward_iterator_tag;

      private:
      friend class ParallelChunkedStorage;

      using storage_iterator = std::conditional_t<isConst, typename ChunkedStorage<T>::const_iterator, typename ChunkedStorage<T>::iterator>;
      using storage_type = std::conditional_t<isConst, const ChunkedStorage<T>, ChunkedStorage<T>>;

      /// The current entry
      LocalChunkedStorageEntry* currentEntry = nullptr;
      /// The iterator of the current entry
      storage_iterator it;

      /// Make sure that the iterator points to a valid element or the end
      void skipEmpty() {
         while (currentEntry && it == currentEntry->storage.end()) {
            currentEntry = currentEntry->next;
            if (currentEntry)
               it = static_cast<storage_type&>(currentEntry->storage).begin();
            else
               it = {};
         }
      }

      /// Constructor
      Iterator(LocalChunkedStorageEntry* entry, storage_iterator it) : currentEntry(entry), it(it) {
         skipEmpty();
      }

      public:
      /// Default constructor
      Iterator() = default;

      /// Dereference
      reference operator*() const {
         return *it;
      }
      /// Dereference
      pointer operator->() const {
         return it.operator->();
      }

      /// Pre-increment
      Iterator& operator++() {
         ++it;
         skipEmpty();
         return *this;
      }
      /// Post-increment
      Iterator operator++(int) {
         Iterator it(*this);
         operator++();
         return it;
      }

      /// Equality comparison
      bool operator==(const Iterator& other) const = default;
   };

   /// A parallel iterator over all chunks in a `ParallelChunkedStorage`
   template <bool isConst>
   class ParallelIterator {
      public:
      /// A range of elements over which a thread iterates exclusively
      class Range {
         public:
         /// The iterator of a range
         class Iterator {
            public:
            using difference_type = std::ptrdiff_t;
            using value_type = std::conditional_t<isConst, const T, T>;
            using pointer = value_type*;
            using reference = value_type&;
            using iterator_category = std::contiguous_iterator_tag;

            private:
            friend class ParallelIterator;

            /// The chunk
            ChunkHeader* chunk = nullptr;
            /// The current index in the chunk
            size_t elementIndex = 0;

            /// Constructor
            Iterator(ChunkHeader* chunk, size_t elementIndex) : chunk(chunk), elementIndex(elementIndex) {}

            public:
            /// Default constructor
            Iterator() = default;

            /// Dereference
            reference operator*() const {
               return chunk->getElements()[elementIndex];
            }
            /// Dereference
            pointer operator->() const {
               return &operator*();
            }

            /// Pre-increment
            Iterator& operator++() {
               ++elementIndex;
               return *this;
            }
            /// Post-increment
            Iterator operator++(int) {
               Iterator it(*this);
               operator++();
               return it;
            }

            /// Pre-decrement
            Iterator& operator--() {
               --elementIndex;
               return *this;
            }
            /// Post-decrement
            Iterator operator--(int) {
               Iterator it(*this);
               operator--();
               return it;
            }

            /// Addition assignment
            Iterator& operator+=(difference_type n) {
               elementIndex += n;
               return *this;
            }
            /// Addition
            Iterator operator+(difference_type n) const {
               Iterator it(*this);
               it += n;
               return it;
            }
            /// Reverse addition
            friend Iterator operator+(difference_type n, const Iterator& it) {
               return it + n;
            }

            /// Subtraction assignment
            Iterator& operator-=(difference_type n) {
               elementIndex -= n;
               return *this;
            }
            /// Subtraction
            Iterator operator-(difference_type n) const {
               Iterator it(*this);
               it -= n;
               return it;
            }

            /// Subtraction between two iterators
            difference_type operator-(const Iterator& other) const {
               return elementIndex - other.elementIndex;
            }

            /// Random access
            reference operator[](difference_type n) const {
               return chunk->getElements()[elementIndex + n];
            }

            /// Equality comparison
            bool operator==(const Iterator& other) const {
               return elementIndex == other.elementIndex;
            }
            /// Three-way comparison
            auto operator<=>(const Iterator& other) const {
               return elementIndex <=> other.elementIndex;
            }
         };

         private:
         friend class ParallelIterator;

         /// The chunk for this range
         ChunkHeader* chunk = nullptr;

         /// Constructor
         Range(ChunkHeader* chunk) : chunk(chunk) {}

         public:
         /// Constructor
         Range() = default;

         /// Get the begin iterator
         Iterator begin() const {
            return Iterator(chunk, 0);
         }

         /// Get the end iterator
         Iterator end() const {
            if (chunk)
               return Iterator(chunk, chunk->numElements);
            else
               return Iterator(chunk, 0);
         }
      };

      private:
      friend class ParallelChunkedStorage;

      /// An entry in the vector of all thread-local chunked storages that
      /// we iterate through.
      struct IterationEntry {
         /// The next available chunk, starts with the last chunk since we
         /// iterate backwards
         ChunkHeader* nextChunk = nullptr;
         /// The index of the next non-empty thread. This will only be written
         /// and read from the same thread as an optimization to quickly skip
         /// over known empty threads.
         size_t nextThreadIndex = static_cast<size_t>(~0ull);
      };

      /// The mapping between thread ids and entry indexes
      std::unordered_map<uint32_t, size_t> threadIdMap;
      /// The iteration entries
      std::vector<IterationEntry> iterationEntries;

      /// Constructor
      explicit ParallelIterator(const ParallelChunkedStorage& storage) {
         iterationEntries.resize(storage.numEntries);
         for (auto* entry = storage.frontEntry; entry; entry = entry->next) {
            threadIdMap.emplace(entry->threadId, entry->index);
            auto& iterationEntry = iterationEntries[entry->index];
            iterationEntry.nextChunk = entry->storage.backChunk;
            iterationEntry.nextThreadIndex = entry->index;
         }
      }

      /// Get the next range concurrently, optimized for the thread with the
      /// given index.
      std::optional<Range> nextImpl(size_t threadIndex) {
         auto& threadEntry = iterationEntries[threadIndex];
         if (threadEntry.nextThreadIndex == static_cast<size_t>(~0ull))
            return {};

         while (threadEntry.nextThreadIndex != static_cast<size_t>(~0ull)) {
            auto& entry = iterationEntries[threadEntry.nextThreadIndex];
            // TODO: This should be atomic_ref, but libc++ hasn't implemented it, yet
            auto& entryNextChunk = reinterpret_cast<std::atomic<ChunkHeader*>&>(entry.nextChunk);
            auto* chunk = entryNextChunk.load();
            if (chunk) {
               while (true) {
                  // We iterate backwards, so replace `nextChunk` with its prev pointer
                  if (entryNextChunk.compare_exchange_weak(chunk, chunk->prev))
                     return Range(chunk);
                  if (!chunk)
                     // There are no chunks left for the current thread
                     break;
               }
            }

            ++threadEntry.nextThreadIndex;
            if (threadEntry.nextThreadIndex >= iterationEntries.size())
               threadEntry.nextThreadIndex = 0;

            if (threadEntry.nextThreadIndex == threadIndex)
               break;
         }

         // We iterated through the entire list and found no entries
         threadEntry.nextThreadIndex = static_cast<size_t>(~0ull);
         return {};
      }

      public:
      /// Default constructor
      ParallelIterator() = default;

      /// Get the next range concurrently, optimized for the thread that
      /// created the given storage ref
      std::optional<Range> next(LocalChunkedStorageRef storageRef) {
         return nextImpl(storageRef.index);
      }

      /// Get the next range concurrently, optimized for the thread with the
      /// given thread id
      std::optional<Range> next(uint32_t threadId) {
         auto it = threadIdMap.find(threadId);
         if (it != threadIdMap.end())
            return nextImpl(it->second);
         else
            return nextImpl(0);
      }

      /// Get the next range concurrently
      std::optional<Range> next() {
         return nextImpl(0);
      }
   };

   public:
   using iterator = Iterator<false>;
   using const_iterator = Iterator<true>;
   using parallel_iterator = ParallelIterator<false>;
   using const_parallel_iterator = ParallelIterator<true>;

   private:
   /// The first entry in the list of chunked storages
   LocalChunkedStorageEntry* frontEntry = nullptr;
   /// The total number of entries
   size_t numEntries = 0;

   public:
   /// Default constructor
   ParallelChunkedStorage() = default;
   /// Destructor
   ~ParallelChunkedStorage() {
      clear();
   }

   /// Move constructor
   ParallelChunkedStorage(ParallelChunkedStorage&& other) noexcept : frontEntry(other.frontEntry), numEntries(other.numEntries) {
      other.frontEntry = nullptr;
      other.numEntries = 0;
   }

   /// Move assignment
   ParallelChunkedStorage& operator=(ParallelChunkedStorage&& other) noexcept {
      if (this == &other)
         return *this;

      clear();

      frontEntry = other.frontEntry;
      numEntries = other.numEntries;
      other.frontEntry = nullptr;
      other.numEntries = 0;
   }

   /// Remove all entries
   void clear() noexcept {
      while (frontEntry) {
         auto* next = frontEntry->next;
         delete frontEntry;
         frontEntry = next;
      }
      numEntries = 0;
   }

   /// The total number of elements. Note that this is not thread-safe and
   /// linear in the number of threads.
   size_t size() const {
      size_t numElements = 0;
      for (auto* entry = frontEntry; entry; entry = entry->next)
         numElements += entry->storage.size();
      return numElements;
   }

   /// Create a new local chunked storage
   LocalChunkedStorageRef createLocalStorage(uint32_t threadId) {
      auto* entry = new LocalChunkedStorageEntry;
      entry->threadId = threadId;
      // TODO: This should be atomic_ref, but libc++ hasn't implemented it, yet
      entry->index = reinterpret_cast<std::atomic<size_t>&>(numEntries).fetch_add(1);
      auto& frontEntryAtomic = reinterpret_cast<std::atomic<LocalChunkedStorageEntry*>&>(frontEntry);

      entry->next = frontEntryAtomic.load();
      while (!frontEntryAtomic.compare_exchange_weak(entry->next, entry))
         ;

      LocalChunkedStorageRef ref;
      ref.storage = &entry->storage;
      ref.index = entry->index;
      return ref;
   }

   /// Get an iterator to the first element
   iterator begin() {
      typename ChunkedStorage<T>::iterator it;
      if (frontEntry)
         it = std::begin(frontEntry->storage);
      return iterator(frontEntry, it);
   }
   /// Get an iterator to the first element
   const_iterator begin() const {
      typename ChunkedStorage<T>::const_iterator it;
      if (frontEntry)
         it = std::cbegin(frontEntry->storage);
      return const_iterator(frontEntry, it);
   }

   /// Get the end iterator
   iterator end() {
      return {};
   }
   /// Get the end iterator
   const_iterator end() const {
      return {};
   }

   /// Get a parallel iterator
   parallel_iterator parallelIter() {
      return parallel_iterator(*this);
   }
   /// Get a parallel iterator
   const_parallel_iterator parallelIter() const {
      return const_parallel_iterator(*this);
   }
};
//---------------------------------------------------------------------------
}
#endif
