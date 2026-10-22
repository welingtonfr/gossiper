cmake_minimum_required(VERSION 3.14)
set(PROJECT_NAME "gossiper")
project(${PROJECT_NAME} LANGUAGES CXX)

# Bundle the pre-built DLL alongside the Flutter app.
# The DLL is built by CI from native/memberlist_wrapper/wrapper.go.
set(gossiper_bundled_libraries
  "${CMAKE_CURRENT_SOURCE_DIR}/../blobs/windows/gossiper.dll"
  PARENT_SCOPE
)
