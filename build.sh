#!/bin/sh
g++ -std=c++17 -O2 -o UdpServer main.cpp -lboost_system -lboost_program_options -lpthread