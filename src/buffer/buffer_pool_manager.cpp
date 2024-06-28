//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> l(latch_);
  frame_id_t frame_id = INVALID_FRAME_ID;
  // try to get a frame in free list or replacer
  if (!free_list_.empty()) {
    frame_id = *free_list_.begin();
    free_list_.pop_front();
  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&frame_id);
    page_table_.erase(pages_[frame_id].page_id_);
  }
  // not found valid frame
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;
  }
  // write data to disk
  if (pages_[frame_id].page_id_ != INVALID_PAGE_ID && pages_[frame_id].is_dirty_) {
    auto p = disk_scheduler_->CreatePromise();
    auto f = p.get_future();
    disk_scheduler_->Schedule({true, pages_[frame_id].data_, pages_[frame_id].page_id_, std::move(p)});
    f.get();
    pages_[frame_id].ResetMemory();
  }
  // get page id
  *page_id = AllocatePage();
  // set metadata
  page_table_[*page_id] = frame_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = *page_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return pages_ + frame_id;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> l(latch_);
  auto it = page_table_.find(page_id);
  frame_id_t frame_id = INVALID_FRAME_ID;
  // in buffer pool
  if (it != page_table_.end()) {
    frame_id = it->second;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return pages_ + frame_id;
  }

  // not in buffer pool, try to get a frame in free list or replacer
  if (!free_list_.empty()) {
    frame_id = *free_list_.begin();
    free_list_.pop_front();
  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&frame_id);
    page_table_.erase(pages_[frame_id].page_id_);
  }
  // not found valid frame
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;
  }
  // write data to disk
  if (pages_[frame_id].page_id_ != INVALID_PAGE_ID && pages_[frame_id].is_dirty_) {
    auto p = disk_scheduler_->CreatePromise();
    auto f = p.get_future();
    disk_scheduler_->Schedule({true, pages_[frame_id].data_, pages_[frame_id].page_id_, std::move(p)});
    f.get();
    pages_[frame_id].ResetMemory();
  }
  // read data from disk
  auto p = disk_scheduler_->CreatePromise();
  auto f = p.get_future();
  disk_scheduler_->Schedule({false, pages_[frame_id].data_, page_id, std::move(p)});
  f.get();
  // set metadata
  page_table_[page_id] = frame_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = page_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> l(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto frame_id = it->second;
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  pages_[frame_id].is_dirty_ |= is_dirty;
  --pages_[frame_id].pin_count_;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> l(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto frame_id = it->second;
  auto p = disk_scheduler_->CreatePromise();
  auto f = p.get_future();
  disk_scheduler_->Schedule({true, pages_[frame_id].data_, page_id, std::move(p)});
  f.get();
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> l(latch_);
  std::vector<std::future<bool>> f(page_table_.size());
  int i = 0;
  for (auto it = page_table_.begin(); it != page_table_.end(); ++it, ++i) {
    auto page_id = it->first;
    auto frame_id = it->second;
    auto p = disk_scheduler_->CreatePromise();
    f[i] = p.get_future();
    disk_scheduler_->Schedule({true, pages_[frame_id].data_, page_id, std::move(p)});
  }
  i = 0;
  for (auto it = page_table_.begin(); it != page_table_.end(); ++it, ++i) {
    f[i].get();
    pages_[it->first].ResetMemory();
    pages_[it->first].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> l(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  auto frame_id = it->second;
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  if (pages_[frame_id].is_dirty_) {
    auto p = disk_scheduler_->CreatePromise();
    auto f = p.get_future();
    disk_scheduler_->Schedule({true, pages_[frame_id].data_, page_id, std::move(p)});
    f.get();
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].ResetMemory();
  free_list_.emplace_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
