#include "client.h"
#include "toolbox.h"
#include <fstream>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>
#include "config.h"
#include <iomanip>
#include <iostream>
#include <chrono>
#include <algorithm>
#include <random>

int main(int argc, char **argv)
{
    char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);
    std::string sys_config_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../config/parameterConfiguration.xml";
    //std::string sys_config_path = "/home/GuanTian/lql/Stripe-plus-plus/project/config/parameterConfiguration.xml";
    std::cout << "Current working directory: " << sys_config_path << std::endl;

    const ECProject::Config *config = ECProject::Config::getInstance(sys_config_path);
    std::string client_ip = "10.10.1.1";
    int client_port = 44444;
    ECProject::Client client(client_ip, client_port, config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort), sys_config_path);
    std::cout << client.sayHelloToCoordinatorByGrpc("Client ID: " + client_ip + ":" + std::to_string(client_port)) << std::endl;

    std::vector<int> parameters = client.get_parameters();
    int k = parameters[0];
    int r = parameters[1];
    int z = parameters[2];
    std::string code_type;
    if(parameters[4] == 0){
        code_type = "AzureLRC";
    }
    else if(parameters[4] == 1){
        code_type = "OptimalLRC";
    }
    else if(parameters[4] == 2){
        code_type = "UniformLRC";
    }
    else if(parameters[4] == 3){
        code_type = "UniLRC";
    }
    else if(parameters[4] == 4){
        code_type = "ShuffledUniformLRC";
    }
    else{
        std::cout << "Code type error" << std::endl;
        return -1;
    }
    double block_size = static_cast<double> (parameters[3]) / 1024 / 1024; //MB
    int n = k + r + z;

    int object_size_of_block_cnt_24[10] = {12, 3, 3, 4, 12, 3, 3, 2, 4, 2};
    int object_size_of_block_cnt_48[10] = {10, 4, 2, 6, 11, 5, 1, 3, 2, 4};
    int object_size_of_block_cnt_72[10] = {3, 18, 4, 4, 4, 5, 3, 20, 5, 6};
    int object_size_of_block_cnt_96[10] = {5, 7, 4, 21, 7, 5, 9, 7, 7, 24};
    int object_size_of_block_cnt[10];
    if (k == 24)
    {
        std::copy(std::begin(object_size_of_block_cnt_24), std::end(object_size_of_block_cnt_24), std::begin(object_size_of_block_cnt));
    }
    else if (k == 48)
    {
        std::copy(std::begin(object_size_of_block_cnt_48), std::end(object_size_of_block_cnt_48), std::begin(object_size_of_block_cnt));
    }
    else if (k == 72)
    {
        std::copy(std::begin(object_size_of_block_cnt_72), std::end(object_size_of_block_cnt_72), std::begin(object_size_of_block_cnt));
    }
    else if (k == 96)
    {
        std::copy(std::begin(object_size_of_block_cnt_96), std::end(object_size_of_block_cnt_96), std::begin(object_size_of_block_cnt));
    }
    else
    {
        std::cout << "Unsupported k, r, z combination" << std::endl;
        return -1;
    }


    std::cout << "Start uploading..." << std::endl;
    std::chrono::steady_clock::time_point upload_start = std::chrono::steady_clock::now();
    for(int i = 0; i < 10; i++){
        std::string object_id = "object_" + std::to_string(i);
        size_t object_size = object_size_of_block_cnt[i] * block_size * 1024 * 1024;
        std::unique_ptr<char[]> data(new char[object_size]);
        std::cout << "Uploading object: " << object_id << " with size: " << object_size << std::endl;
        bool res = client.upload_object(object_id, std::move(data), object_size);
        if (res) {
            std::cout << "Upload object " << object_id << " successfully!" << std::endl;
        } else {
            std::cout << "Failed to upload object " << object_id << std::endl;
        }
    }    
    std::chrono::steady_clock::time_point upload_end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = upload_end - upload_start;
    std::chrono::steady_clock::time_point get_start = std::chrono::steady_clock::now();
    for(int i = 0; i < 10; i++){
        std::string object_id = "object_" + std::to_string(i);
        size_t object_size = object_size_of_block_cnt[i] * block_size * 1024 * 1024;
        std::unique_ptr<char[]> data(new char[object_size]);
        std::cout << "getting object: " << object_id << " with size: " << object_size << std::endl;
        std::shared_ptr<char[]> retrieved_data = client.get_object(object_id, object_size_of_block_cnt[i]);
        if(retrieved_data.get() != nullptr) {
            std::cout << "Retrieved object " << object_id << " successfully!" << std::endl;
        } else {
            std::cout << "Failed to retrieve object " << object_id << std::endl;
        }
    }
    std::chrono::steady_clock::time_point get_end = std::chrono::steady_clock::now();
    std::chrono::duration<double> get_elapsed_seconds = get_end - get_start;
    double total_data_size = k * block_size; // total data size for 10 objects
    std::cout << "Upload time: " << elapsed_seconds.count() << " seconds" << std::endl;
    std::cout << "Upload throughput: " << total_data_size / elapsed_seconds.count() << " MB/s" << std::endl;
    std::cout << "Get time: " << get_elapsed_seconds.count() << " seconds" << std::endl;
    std::cout << "Get throughput: " << total_data_size / get_elapsed_seconds.count() << " MB/s" << std::endl;

    return 0;
}
