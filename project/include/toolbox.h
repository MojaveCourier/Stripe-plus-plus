#ifndef TOOLBOX_H
#define TOOLBOX_H
#include <iostream>
#include <string>
#include <vector>
#include <unordered_set>
#include <proxy.pb.h>
#include <coordinator.pb.h>
#define MAX_KEY_LENGTH 200
#define MAX_VALUE_LENGTH 20000
namespace ECProject
{
    class ToolBox
    {
    private:
        static ToolBox *instance;
        ToolBox() {}
        ToolBox(const ToolBox &) = delete;
        ToolBox &operator=(const ToolBox &) = delete;

    public:
        static ToolBox *getInstance()
        {
            if (instance == nullptr)
            {
                instance = new ToolBox();
            }
            return instance;
        }

        bool random_generate_kv(std::string &key, std::string &value,
                                int key_length = 0, int value_length = 0);
        bool random_generate_value(std::string &value, int value_length = 0);
        std::vector<unsigned char> int_to_bytes(int);
        int bytes_to_int(std::vector<unsigned char> &bytes);
        std::string gen_key(int key_len, std::unordered_set<std::string> keys);
        std::vector<char *> splitCharPointer(const char *str, const size_t str_size, const std::vector<size_t> &sizes);
        std::vector<char *> splitCharPointer(const char *str, const std::shared_ptr<proxy_proto::AppendStripeDataPlacement> append_stripe_data_placement);
        std::vector<char *> splitCharPointer(const char *str, const coordinator_proto::ReplyProxyIPsPorts *reply_proxy_ips_ports);
        std::string gen_append_key(int stripe_id, int group_id);
        void remove_common_zeros(std::vector<int>& vec1, std::vector<int>& vec2, std::vector<int>& vec3);
    };

} // namespace ECProject
#endif