cmake_minimum_required(VERSION 4.0)


project(rtipc-dbus LANGUAGES C)

set(CMAKE_C_STANDARD 11)



add_subdirectory(rtipc)

project(rtipc-dbus LANGUAGES C)

find_package(PkgConfig REQUIRED)
pkg_check_modules(SYSTEMD REQUIRED IMPORTED_TARGET libsystemd)


add_executable(server server/main.c server/service.c server/server.c messages.c)

target_include_directories(server PRIVATE ${CMAKE_CURRENT_LIST_DIR} ${CMAKE_CURRENT_LIST_DIR}/rtipc/lib ${SYSTEMD_INCLUDE_DIR})
target_compile_definitions(server PUBLIC -DRTIPC_LOG_API=1)
target_link_libraries(server PRIVATE rtipc PkgConfig::SYSTEMD)


add_executable(client client/main.c client/client.c messages.c)

target_include_directories(client PRIVATE ${CMAKE_CURRENT_LIST_DIR} ${CMAKE_CURRENT_LIST_DIR}/rtipc/lib ${SYSTEMD_INCLUDE_DIR})
target_compile_definitions(client PUBLIC -DRTIPC_LOG_API=1)
target_link_libraries(client PRIVATE rtipc PkgConfig::SYSTEMD)
