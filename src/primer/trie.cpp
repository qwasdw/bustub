#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  auto node = root_;
  if (node == nullptr) {
    return nullptr;
  }
  for (char c : key) {
    auto next = node->children_.find(c);
    if (next != node->children_.end()) {
      node = next->second;
    } else {
      return nullptr;
    }
  }
  if (node == nullptr || !node->is_value_node_) {
    return nullptr;
  }
  auto res = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (res != nullptr) {
    return res->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::shared_ptr<T> shared_value = std::make_shared<T>(std::move(value));
  std::shared_ptr<TrieNode> new_trie = nullptr;
  auto cur_trie = root_;
  // key is empty
  if (key.empty()) {
    if (cur_trie) {
      new_trie = std::make_shared<TrieNodeWithValue<T>>(cur_trie->children_, shared_value);
    } else {
      new_trie = std::make_shared<TrieNodeWithValue<T>>(shared_value);
    }
    return Trie(new_trie);
  }

  // key not empty
  if (cur_trie) {
    new_trie = cur_trie->Clone();
  } else {
    new_trie = std::make_shared<TrieNode>();
  }
  std::shared_ptr<TrieNode> new_trie_next = new_trie;
  size_t index = 0;
  for (; index < key.size(); ++index) {
    if (cur_trie) {
      auto it = cur_trie->children_.find(key[index]);
      if (it != cur_trie->children_.end()) {
        cur_trie = it->second;
      } else {
        cur_trie = nullptr;
      }
    }
    std::shared_ptr<TrieNode> child_trie = nullptr;
    if (index + 1 < key.size()) {
      if (cur_trie) {
        child_trie = cur_trie->Clone();
      } else {
        child_trie = std::make_shared<TrieNode>();
      }
    } else {
      if (cur_trie) {
        child_trie = std::make_shared<TrieNodeWithValue<T>>(cur_trie->children_, shared_value);
      } else {
        child_trie = std::make_shared<TrieNodeWithValue<T>>(shared_value);
      }
    }
    new_trie_next->children_[key[index]] = child_trie;
    new_trie_next = child_trie;
  }
  return Trie(new_trie);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  //  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::vector<std::shared_ptr<TrieNode>> st;
  auto cur_trie = root_;
  std::shared_ptr<TrieNode> new_trie = nullptr;
  // key is empty
  if (key.empty()) {
    if (cur_trie) {
      new_trie = std::make_shared<TrieNode>(cur_trie->children_);
    }
    return Trie(new_trie);
  }

  // key not empty
  if (cur_trie) {
    new_trie = cur_trie->Clone();
    st.emplace_back(new_trie);
    std::shared_ptr<TrieNode> child_trie = new_trie;
    std::shared_ptr<TrieNode> next = nullptr;
    size_t index = 0;
    // copy node on path
    for (; index < key.size(); ++index) {
      auto it = cur_trie->children_.find(key[index]);
      if (it != cur_trie->children_.end()) {
        cur_trie = it->second;
        if (index + 1 != key.size()) {
          next = cur_trie->Clone();
        } else {
          next = std::make_shared<TrieNode>(cur_trie->children_);
        }
        child_trie->children_[key[index]] = next;
        st.emplace_back(next);
        child_trie = next;
      } else {
        break;
      }
    }

    // key not exist
    if (st.size() - 1 != key.size()) {
      return Trie(root_);
    }

    // remove null node
    for (int i = index - 1; i >= 0; --i) {
      if (st[i]->children_[key[i]]->children_.empty()) {
        if (st[i]->children_[key[i]]->is_value_node_) {
          break;
        }
        st[i]->children_.erase(key[i]);
      } else {
        break;
      }
    }
    if (new_trie->children_.empty() && !new_trie->is_value_node_) {
      new_trie = nullptr;
    }
  }
  return Trie(new_trie);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
