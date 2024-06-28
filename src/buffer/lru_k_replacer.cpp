//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> l(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  bool found = false;
  size_t diff = 0;
  size_t earliest_timestamp = 0;
  for (auto it = node_store_.begin(); it != node_store_.end(); ++it) {
    if (it->second.is_evictable_) {
      if (found) {
        if (diff == 0) {
          if (it->second.k_ < k_ && earliest_timestamp > *it->second.history_.begin()) {
            diff = 0;
            *frame_id = it->first;
            earliest_timestamp = *it->second.history_.begin();
          }
        } else {
          if (it->second.k_ < k_) {
            diff = 0;
            *frame_id = it->first;
            earliest_timestamp = *it->second.history_.begin();
          } else {
            size_t cur_diff = current_timestamp_ - *it->second.history_.begin();
            if (cur_diff > diff) {
              diff = cur_diff;
              *frame_id = it->first;
              earliest_timestamp = *it->second.history_.begin();
            }
          }
        }
      } else {
        *frame_id = it->first;
        found = true;
        earliest_timestamp = *it->second.history_.begin();
        if (it->second.k_ == k_) {
          diff = current_timestamp_ - earliest_timestamp;
        }
      }
    }
  }
  if (found) {
    node_store_.erase(*frame_id);
    --curr_size_;
  }
  return found;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id > static_cast<int32_t>(replacer_size_)) {
    throw Exception("RecordAccess: frame_id is bigger than replacer_size_");
  }
  std::unique_lock<std::mutex> l(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    LRUKNode node;
    node.k_ = 1;
    node.fid_ = frame_id;
    node.history_.emplace_back(current_timestamp_);
    node_store_[frame_id] = node;
  } else {
    if (it->second.k_ == k_) {
      it->second.history_.pop_front();
      --it->second.k_;
    }
    it->second.history_.emplace_back(current_timestamp_);
    ++it->second.k_;
  }
  ++current_timestamp_;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> l(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    throw Exception("SetEvictable: frame id is invalid");
  }
  if (it->second.is_evictable_ && !set_evictable) {
    --curr_size_;
  } else if (!it->second.is_evictable_ && set_evictable) {
    ++curr_size_;
  }
  it->second.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> l(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  if (!it->second.is_evictable_) {
    throw Exception("Remove: is called on a non-evictable frame");
  }
  node_store_.erase(frame_id);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  size_t res = curr_size_;
  latch_.unlock();
  return res;
}

}  // namespace bustub
