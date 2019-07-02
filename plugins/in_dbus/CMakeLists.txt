set(src
  in_dbus.c)

pkg_check_modules(DBUS REQUIRED dbus-1)
include_directories(${DBUS_INCLUDE_DIRS})

FLB_PLUGIN(in_dbus "${src}" "")
target_link_libraries(flb-plugin-in_dbus ${DBUS_LIBRARIES})
