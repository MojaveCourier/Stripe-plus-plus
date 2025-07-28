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
#include <ec_encoder.h>

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
    int object_size_of_block_cnt_24[10] = {6, 2, 1, 1, 4, 2, 1, 1, 2, 4};
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
    size_t object_size = k * parameters[3];

    /*
    // for recovery test
    std::vector<double> recovery_times;
    std::unique_ptr<char[]> data(new char[object_size]);
    client.upload_object("object_1", std::move(data), object_size);
    sleep(2); // wait for the upload to complete
    for(int i = 0; i < k; i++){
        std::chrono::steady_clock::time_point recovery_start = std::chrono::steady_clock::now();
        bool res = client.recovery(0, i);
        if(!res){
            std::cout << "Recovery Failed!" << std::endl;
        }
        std::chrono::steady_clock::time_point recovery_end = std::chrono::steady_clock::now();
        std::chrono::duration<double> recovery_duration = recovery_end - recovery_start;
        recovery_times.push_back(recovery_duration.count());
    }
    double avg_recovery_time = std::accumulate(recovery_times.begin(), recovery_times.end(), 0.0) / recovery_times.size();
    double max_recovery_time = *std::max_element(recovery_times.begin(), recovery_times.end());
    double min_recovery_time = *std::min_element(recovery_times.begin(), recovery_times.end());
    std::cout << "Average Recovery Time: " << avg_recovery_time << " seconds" << std::endl;
    std::cout << "Max Recovery Time: " << max_recovery_time << " seconds" << std::endl;
    std::cout << "Min Recovery Time: " << min_recovery_time << " seconds" << std::endl;
    */
    
    
    /*std::vector<double> write_spans;
    // for write test
    double total_data_size = k * block_size; // total data size for 10 objects
    std::cout << "Start uploading..." << std::endl;
    for(int j = 0; j < 5; j++){
        std::chrono::steady_clock::time_point upload_start = std::chrono::steady_clock::now();
        for(int i = 0; i < 10; i++){
            std::string object_id = "object_" + std::to_string(j * 10 + i);
            size_t object_size = parameters[3] * object_size_of_block_cnt[i];
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
        write_spans.push_back(elapsed_seconds.count());
    }
    double avg_write_span = std::accumulate(write_spans.begin(), write_spans.end(), 0.0) / write_spans.size();
    double max_write_span = *std::max_element(write_spans.begin(), write_spans.end());
    double min_write_span = *std::min_element(write_spans.begin(), write_spans.end());
    double avg_write_bandwidth = total_data_size / avg_write_span;
    double max_write_bandwidth = total_data_size / min_write_span;
    double min_write_bandwidth = total_data_size / max_write_span;
    std::cout << "Average Write Span: " << avg_write_span << " seconds" << std::endl;
    std::cout << "Max Write Span: " << max_write_span << " seconds" << std::endl;
    std::cout << "Min Write Span: " << min_write_span << " seconds" << std::endl;
    std::cout << "Average Write Throughput: " << avg_write_bandwidth << " MB/s" << std::endl;
    std::cout << "Max Write Throughput: " << max_write_bandwidth << " MB/s" << std::endl;
    std::cout << "Min Write Throughput: " << min_write_bandwidth << " MB/s" << std::endl;
    */
   
    /*
    // for read test
    std::vector<double> read_spans;
    for(int j = 0; j < 5; j++){
        std::chrono::steady_clock::time_point get_start = std::chrono::steady_clock::now();
        for(int i = 0; i < 10; i++){
            std::string object_id = "object_" + std::to_string(j * 10 + i);
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
        read_spans.push_back(get_elapsed_seconds.count());
    }

    double avg_read_span = std::accumulate(read_spans.begin(), read_spans.end(), 0.0) / read_spans.size();
    double max_read_span = *std::max_element(read_spans.begin(), read_spans.end());
    double min_read_span = *std::min_element(read_spans.begin(), read_spans.end());

    double avg_read_bandwidth = total_data_size / avg_read_span;
    double max_read_bandwidth = total_data_size / min_read_span;
    double min_read_bandwidth = total_data_size / max_read_span;
    std::cout << "Average Read Span: " << avg_read_span << " seconds" << std::endl;
    std::cout << "Max Read Span: " << max_read_span << " seconds" << std::endl;
    std::cout << "Min Read Span: " << min_read_span << " seconds" << std::endl;
    std::cout << "Average Read Throughput: " << avg_read_bandwidth << " MB/s" << std::endl;
    std::cout << "Max Read Throughput: " << max_read_bandwidth << " MB/s" << std::endl;
    std::cout << "Min Read Throughput: " << min_read_bandwidth << " MB/s" << std::endl;
    */

    /*
    // for IBM workload
    int cur_read_id = 0;
    int cur_write_id = 0;
    std::string load_trace_path =  std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../trace/IBM_put_post.txt";
    std::fstream load_trace_file(load_trace_path);
    std::string load_trace_line;
    std::vector<double> work_time;
    while(std::getline(load_trace_file, load_trace_line)){
        std::string operation;
        int file_block_cnt;
        std::istringstream iss(load_trace_line);
        std::getline(iss, operation, ' ');
        iss >> file_block_cnt;
        if(operation != "REST.PUT.OBJECT" ){
            std::cout << "Unsupported operation: " << operation << std::endl;
            return -1;
        }
        std::string object_key = "object_" + std::to_string(cur_write_id++);
        size_t file_size = file_block_cnt * block_size * 1024 * 1024; // MB to bytes
        std::unique_ptr<char[]> data(new char[file_size]);
        std::cout << "Uploading object: " << object_key << " with size: " << file_block_cnt * block_size << " MB" << std::endl;
        std::chrono::steady_clock::time_point upload_start = std::chrono::steady_clock::now();
        bool res = client.upload_object(object_key, std::move(data), file_block_cnt * block_size * 1024 * 1024);
        std::chrono::steady_clock::time_point upload_end = std::chrono::steady_clock::now();
        std::chrono::duration<double> upload_duration = upload_end - upload_start;
        work_time.push_back(upload_duration.count());
        if (res) {
            std::cout << "Upload object " << object_key << " successfully!" << std::endl;
        } else {
            std::cout << "Failed to upload object " << object_key << std::endl;
            return -1;
        }
    }
    std::string output_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../IBM_work_output.txt";
    std::ofstream output_file(output_path);
    if (!output_file.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return -1;
    }
    for (const auto &time : work_time) {
        output_file << std::fixed << std::setprecision(6) << time << std::endl;
    }
    output_file.close();
    std::cout << "Work time output saved to: " << output_path << std::endl;
    */        
    return 0;
}
