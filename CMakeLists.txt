cmake_minimum_required(VERSION 3.16)
project(picam_orchestrator CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Release CACHE STRING "")

find_package(JPEG REQUIRED)

message(STATUS "libjpeg : ${JPEG_LIBRARIES}")

add_executable(picam_orchestrator src/main.cpp)

target_include_directories(picam_orchestrator PRIVATE
        src
    ${JPEG_INCLUDE_DIRS}
)

target_link_libraries(picam_orchestrator PRIVATE
    ${JPEG_LIBRARIES}
    pthread
)

target_compile_options(picam_orchestrator PRIVATE
    -O3 -Wall -Wextra -pthread
)

install(TARGETS picam_orchestrator DESTINATION /usr/bin)
