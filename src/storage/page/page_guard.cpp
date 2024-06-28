#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.Reset();
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  Reset();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.Reset();
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  auto new_bpm = bpm_;
  auto new_page = page_;
  Reset();
  new_page->RLatch();
  return {new_bpm, new_page};
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  auto new_bpm = bpm_;
  auto new_page = page_;
  Reset();
  new_page->WLatch();
  return {new_bpm, new_page};
}

void BasicPageGuard::Reset() {
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {
  guard_.bpm_ = bpm;
  guard_.page_ = page;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  that.guard_.Reset();
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  that.guard_.Reset();
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
  guard_.Reset();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {
  guard_.bpm_ = bpm;
  guard_.page_ = page;
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  that.guard_.Reset();
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  that.guard_.Reset();
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.is_dirty_ = true;
    guard_.Drop();
  }
  guard_.Reset();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
