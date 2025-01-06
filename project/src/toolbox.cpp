#include "toolbox.h"
#include <ctime>
#include <cstring>
#include <random>
#include <unordered_set>
#include <cassert>

namespace ECProject
{

  ToolBox *ToolBox::instance = nullptr;

  std::vector<char *> ToolBox::splitCharPointer(const char *str, const size_t str_size, const std::vector<size_t> &sizes)
  {
    std::vector<char *> result;
    size_t currentOffset = 0;

    for (size_t i = 0; i < sizes.size(); ++i)
    {
      // slice size is valid
      std::cout << "[ToolBox::splitCharPointer] currentOffset: " << currentOffset << " i: " << i << " sizes[i]: " << sizes[i] << " str_size: " << str_size << std::endl;
      assert(currentOffset + sizes[i] <= str_size && "splitCharPointer: Invalid offset provided.");
      result.push_back(const_cast<char *>(str + currentOffset));
      currentOffset += sizes[i];
    }

    assert(currentOffset == str_size && "The buf is not fully devided!");

    return result;
  }

  std::vector<char *> ToolBox::splitCharPointer(const char *str, const std::shared_ptr<proxy_proto::AppendStripeDataPlacement> append_stripe_data_placement)
  {
    std::vector<size_t> sizes;
    for (int i = 0; i < append_stripe_data_placement->sizes_size(); i++)
    {
      sizes.push_back(append_stripe_data_placement->sizes(i));
    }
    return splitCharPointer(str, append_stripe_data_placement->append_size(), sizes);
  }

  std::vector<char *> ToolBox::splitCharPointer(const char *str, const coordinator_proto::ReplyProxyIPsPorts *reply_proxy_ips_ports)
  {
    std::vector<size_t> sizes;
    for (int i = 0; i < reply_proxy_ips_ports->cluster_slice_sizes_size(); i++)
    {
      sizes.push_back(reply_proxy_ips_ports->cluster_slice_sizes(i));
    }
    return splitCharPointer(str, reply_proxy_ips_ports->sum_append_size(), sizes);
  }

  std::string ToolBox::gen_append_key(int stripe_id, int group_id)
  {
    return std::to_string(stripe_id) + "_" + std::to_string(group_id);
  }

  bool ToolBox::random_generate_kv(std::string &key, std::string &value,
                                   int key_length, int value_length)
  {
    /*如果长度为0，则随机生成长度,key的长度不大于MAX_KEY_LENGTH，value的长度不大于MAX_VALUE_LENGTH*/
    /*现在生成的key,value内容是固定的，可以改成随机的(增加参数)*/
    /*如果需要生成的key太多，避免重复生成，可以改成写文件保存下来keyvalue，下次直接读文件的形式，
    但这个需要修改函数参数或者修改run_client的内容了*/

    struct timespec tp;

    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tp);
    srand(tp.tv_nsec);
    if (key_length == 0)
    {
    }
    else
    {
      for (int i = 0; i < key_length; i++)
      {
        key = key + char('a' + rand() % 26);
      }
    }
    if (value_length == 0)
    {
    }
    else
    {
      for (int i = 0; i < value_length / 26; i++)
      {
        for (int j = 65; j <= 90; j++)
        {
          value = value + char(j);
        }
      }
      for (int i = 0; i < value_length - int(value.size()); i++)
      {
        value = value + char('A' + i);
      }
    }
    return true;
  }

  std::vector<unsigned char> ToolBox::int_to_bytes(int integer)
  {
    std::vector<unsigned char> bytes(sizeof(int));
    unsigned char *p = (unsigned char *)(&integer);
    for (int i = 0; i < int(bytes.size()); i++)
    {
      memcpy(&bytes[i], p + i, 1);
    }
    return bytes;
  }

  int ToolBox::bytes_to_int(std::vector<unsigned char> &bytes)
  {
    int integer;
    unsigned char *p = (unsigned char *)(&integer);
    for (int i = 0; i < int(bytes.size()); i++)
    {
      memcpy(p + i, &bytes[i], 1);
    }
    return integer;
  }

  bool ToolBox::random_generate_value(std::string &value, int value_length)
  {
    /*生成一个固定大小的随机value*/
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned int> dis(0, 25);
    for (int i = 0; i < value_length; i++)
    {
      value = value + (dis(gen) % 2 ? char('a' + dis(gen) % 26) : char('A' + dis(gen) % 26));
    }
    return true;
  }
  std::string ToolBox::gen_key(int key_len, std::unordered_set<std::string> keys)
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned int> dis(0, 25);
    std::string key;
    do
    {
      key.clear();
      for (int i = 0; i < key_len; i++)
      {
        key = key + (dis(gen) % 2 ? char('a' + dis(gen) % 26) : char('A' + dis(gen) % 26));
      }
    } while (keys.count(key) > 0);
    return key;
  }
} // namespace ECProject