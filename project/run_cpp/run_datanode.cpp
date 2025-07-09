#include "datanode.h"

int main(int argc, char **argv)
{
    // pid_t pid = fork();
    // if (pid > 0)
    // {
    //     exit(0);
    // }
    // setsid();
    if (true)
    {
        umask(0);
        close(STDIN_FILENO);
        // close(STDOUT_FILENO);
        // close(STDERR_FILENO);
    }

    std::string ip_and_port(argv[1]);
    // std::string ip = ip_and_port.substr(0, ip_and_port.find(":"));
    // int port = std::stoi(ip_and_port.substr(ip_and_port.find(":") + 1, ip_and_port.size()));
    char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);
    std::string sys_config_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../config/parameterConfiguration.xml";
    // std::string sys_config_path = "/home/GuanTian/lql/Stripe-plus-plus/project/config/parameterConfiguration.xml";
    ECProject::DataNode datanode(ip_and_port, sys_config_path);
    datanode.Run();
    return 0;
}
